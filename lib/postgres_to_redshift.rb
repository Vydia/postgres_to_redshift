require "postgres_to_redshift/version"
require 'pg'
require 'uri'
require 'aws-sdk-v1'
require 'zlib'
require 'tempfile'
require "postgres_to_redshift/table"
require "postgres_to_redshift/column"

class PostgresToRedshift

  attr_reader :source_connection, :target_connection, :s3

  KILOBYTE = 1024
  MEGABYTE = KILOBYTE * 1024
  GIGABYTE = MEGABYTE * 1024

  def self.update_tables(batch_size: nil)
    update_tables = PostgresToRedshift.new

    puts "exclude_filters: #{exclude_filters}"
    puts "include_filters: #{include_filters}"
    target_connection.exec("CREATE SCHEMA IF NOT EXISTS #{target_schema}")

    update_tables.tables.each do |table|
      next if exclude_filters.any? { |filter| table.name.downcase.include?(filter.downcase) }

      next unless include_filters.empty? || include_filters.any?{ |filter| table.name.downcase == filter.downcase }

      target_connection.exec("DROP TABLE #{target_schema}.#{table.name}") if drop_table_before_create

      target_connection.exec("CREATE TABLE IF NOT EXISTS #{target_schema}.#{target_connection.quote_ident(table.target_table_name)} (#{table.columns_for_create})")

      update_tables.copy_table(table, batch_size: batch_size)

      update_tables.import_table(table)

      update_tables.clean_up_updating_table(table)
    end
  end

  def self.exclude_filters
    (ENV['POSTGRES_TO_REDSHIFT_EXCLUDE_TABLE_PATTERN'] || "").split(',')
  end

  def self.include_filters
    (ENV['POSTGRES_TO_REDSHIFT_INCLUDE_TABLE_PATTERN'] || "").split(',')
  end

  def self.drop_table_before_create
    ENV['DROP_TABLE_BEFORE_CREATE']
  end

  def self.source_connection
    unless instance_variable_defined?(:"@source_connection")
      @source_connection = PG::Connection.new(ENV['POSTGRES_TO_REDSHIFT_SOURCE_URI'])
      @source_connection.exec("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;")
    end

    @source_connection
  end

  def self.target_connection
    unless instance_variable_defined?(:"@target_connection")
      @target_connection = PG::Connection.new(ENV['POSTGRES_TO_REDSHIFT_TARGET_URI'])
    end

    @target_connection
  end

  def self.target_schema
    ENV.fetch('POSTGRES_TO_REDSHIFT_TARGET_SCHEMA')
  end

  def self.source_schema
    ENV['POSTGRES_TO_REDSHIFT_SOURCE_SCHEMA'] || 'public'
  end

  def target_schema
    self.class.target_schema
  end

  def source_schema
    self.class.source_schema
  end

  def source_connection
    self.class.source_connection
  end

  def target_connection
    self.class.target_connection
  end

  def tables
    source_connection.exec("SELECT * FROM information_schema.tables WHERE table_schema = '#{source_schema}' AND table_type in ('BASE TABLE', 'VIEW') order by table_name").map do |table_attributes|
      table = Table.new(attributes: table_attributes)
      next if table.name =~ /^pg_/
      table.columns = column_definitions(table)
      table
    end.compact
  end

  def column_definitions(table)
    source_connection.exec("SELECT * FROM information_schema.columns WHERE table_schema='#{source_schema}' AND table_name='#{table.name}' order by ordinal_position")
  end

  def s3
    @s3 ||= AWS::S3.new(access_key_id: ENV['S3_DATABASE_EXPORT_ID'], secret_access_key: ENV['S3_DATABASE_EXPORT_KEY'])
  end

  def bucket
    @bucket ||= s3.buckets[ENV['S3_DATABASE_EXPORT_BUCKET']]
  end

  def upload(original_file, s3_path, options = {})
    attachment = options.fetch(:attachment, false)
    s3_options = options.reject{ |key| key == :private || key == :attachment }.merge(
      multipart_threshold: 524288000,
    )

    s3_options[:content_disposition] = "attachment; filename=#{File.basename(s3_path)}" if attachment

    bucket.objects[s3_path].write(
      original_file,
      s3_options,
    )
  end

  def copy_table(table, batch_size: nil)
    tmpfile = Tempfile.new("psql2rs")
    tmpfile.binmode
    zip = Zlib::GzipWriter.new(tmpfile)
    chunksize = 5 * GIGABYTE # uncompressed
    chunk = 1
    # Delete previous exports before starting
    bucket.objects.with_prefix("export/#{table.target_table_name}.psv.gz").delete_all

    # 1. Determine the list of COPY commands to run
    if batch_size.nil?
      # Normal execution: Single command for the whole table
      copy_commands = [
        "COPY (SELECT #{table.columns_for_copy} FROM #{source_schema}.#{table.name}) TO STDOUT WITH DELIMITER ',' QUOTE '''' ENCODING 'UTF8' CSV"
      ]
    else
      # Batched execution: Determine ID ranges
      puts "Determining min/max ID for batched copy on column: id"

      # Get min/max ID from the source table
      id_range_query = "SELECT MIN(id), MAX(id) FROM #{source_schema}.#{table.name}"
      range_result = source_connection.exec(id_range_query).first
      min_id = range_result['min']&.to_i
      max_id = range_result['max']&.to_i

      # If the table is empty or has no IDs, stop
      raise "DeveloperError: Table #{table} does not have an id column." if min_id.nil? || max_id.nil?

      copy_commands = []
      current_id = min_id
      while current_id <= max_id
        next_id = [current_id + batch_size - 1, max_id].min

        # Generate the COPY command with the WHERE clause for the specific ID range
        copy_commands << "COPY (SELECT #{table.columns_for_copy} FROM #{source_schema}.#{table.name} WHERE id BETWEEN #{current_id} AND #{next_id}) TO STDOUT WITH DELIMITER ',' QUOTE '''' ENCODING 'UTF8' CSV"
        current_id = next_id + 1
      end
    end

    # 2. Execute all commands sequentially
    begin
      puts "Downloading #{table} with #{copy_commands.size} #{batch_size.nil? ? 'full query' : 'batches'}"

      copy_commands.each do |copy_command|
        puts "Running: #{copy_command}"
        source_connection.copy_data(copy_command) do
          while row = source_connection.get_copy_data
            row = custom_transform_for_tables(row, table)
            zip.write(row)
            # Write chunk to S3 and restart with new tmp file
            if (zip.pos > chunksize)
              zip.finish
              tmpfile.rewind
              upload_table(table, tmpfile, chunk)
              chunk += 1
              zip.close unless zip.closed?
              tmpfile.unlink
              tmpfile = Tempfile.new("psql2rs")
              tmpfile.binmode
              zip = Zlib::GzipWriter.new(tmpfile)
            end
          end
        end
        puts "Resetting connection: source_connection.reset"
        source_connection.reset
      end
      zip.finish
      tmpfile.rewind
      upload_table(table, tmpfile, chunk)
      source_connection.reset
    ensure
      zip.close unless zip.closed?
      tmpfile.unlink
    end
  end

  def custom_transform_for_tables(row, table)
    return row.gsub(/NaN|Infinity|-Infinity/, "0.0") if table.name == "assets"
    row
  end

  def upload_table(table, buffer, chunk)
    puts "Uploading #{table.target_table_name}.#{chunk}"
    upload(buffer, "export/#{table.target_table_name}.psv.gz.#{chunk}")
  end

  def import_table(table)
    puts "Importing #{table.target_table_name}"
    schema = self.class.target_schema

    target_connection.exec("BEGIN;")

    target_connection.exec("DROP TABLE IF EXISTS #{schema}.#{table.target_table_name}_updating")

    target_connection.exec("ALTER TABLE #{schema}.#{target_connection.quote_ident(table.target_table_name)} RENAME TO #{table.target_table_name}_updating")

    target_connection.exec("CREATE TABLE #{schema}.#{target_connection.quote_ident(table.target_table_name)} (#{table.columns_for_create})")

    target_connection.exec("COPY #{schema}.#{target_connection.quote_ident(table.target_table_name)} FROM 's3://#{ENV['S3_DATABASE_EXPORT_BUCKET']}/export/#{table.target_table_name}.psv.gz' CREDENTIALS 'aws_access_key_id=#{ENV['S3_DATABASE_EXPORT_ID']};aws_secret_access_key=#{ENV['S3_DATABASE_EXPORT_KEY']}' GZIP CSV QUOTE '''' DELIMITER ',' TRUNCATECOLUMNS ACCEPTINVCHARS MAXERROR 2;")

    target_connection.exec("GRANT SELECT, REFERENCES ON #{schema}.#{table.target_table_name} TO GROUP bi, GROUP bi_tools;")

    target_connection.exec("GRANT ALL PRIVILEGES ON #{schema}.#{table.target_table_name} TO GROUP data_engineering;")

    target_connection.exec("COMMIT;")
    puts "Imported #{table.target_table_name}"
  end

  def clean_up_updating_table(table)
    schema = self.class.target_schema
    target_connection.exec("DROP TABLE IF EXISTS #{schema}.#{table.target_table_name}_updating")
  rescue StandardError => error
    puts error.message
  end
end
