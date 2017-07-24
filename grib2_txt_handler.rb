class Grib2TxtHandler
  DIRECTORY = "grib2_txt_files"

  WGRIB2 = "/usr/bin/grib2/wgrib2/wgrib2"

  class ConvertationFailException < StandardError
  end

  attr_reader :record,
              :filename

  def self.convert(grib2file)
    raise "Forecast(\"#{grib2file.path}\") must be fetched" unless grib2file.fetched?

    grib2_records_info = `#{WGRIB2} #{grib2file.path}`

    grib2_txt_files = {}

    grib2_records_info.each_line do |record_info|

      info_fields = record_info.split(":")

      timestamp = info_fields[2].split("=").last

      record = "#{info_fields[3]}:#{info_fields[4]}"

      hour_of_forecast = info_fields[5].split(" ").first.split("-").last

      filename = "#{timestamp}.#{record.gsub(" ", "_")}.#{hour_of_forecast}.txt"

      if not File.exist?(File.join(DIRECTORY, filename))
        unless system("#{WGRIB2} #{grib2file.path} -d #{info_fields[0]} -text #{DIRECTORY}/#{filename}")
          raise ConvertationFailException, "Forecast(\"#{grib2file.path}\") cannot convert to text form. WGRIB2 error"
        end
      end

      grib2_txt_files[record] = self.new(File.join(DIRECTORY, filename), record)
    end

    grib2_txt_files
  end

  def remove 
    @io.close
    File.delete(@path)
  end

  def get_value
    @io.gets.to_f
  end

  def to_s
    @filename
  end

  private

  def initialize(path, record)
    raise ConvertationFailException, "ForecastTxt(\"#{path}\") not exist" unless File.exist?(path)
    @record = record

    @path = path

    @filename = File.basename(path)

    @io = File.open(path)
    
    @io.gets
  end

end

# if __FILE__ == $0
#   fcst =  Grib2FileHandler.new(date: Date.today - 1, 
#   hour_of_run: 0,
#   hour_of_forecast: 6
#   )

#   puts !fcst.try_fetch(attemps: 5).nil?

#   txt = Grib2TxtHandler.transform(fcst)

  

#   h = {}

#   counter = 1

#   (-90..90).step(0.25) do |lat|

#     h[lat] = {}

#     (0...360).step(0.25) do |lon|
#       h[lat][lon] = txt["TMP:2 m above ground"].get
#       counter += 1
#     end
#   end

#   puts "Corr = #{h[50.5][93.5] == 297.981}"

#   puts "lines #{counter}"

#   # [93.500000,50.500000,297.981]

# end