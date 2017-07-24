require "excon"
require "date"

class Grib2FileHandler
  
  class DownloadFailException < StandardError
  end

  # directory storing GRIB2 files
  DIRECTORY = './grib2_files'
  # url of GFS server
  SERVER = 'http://www.ftp.ncep.noaa.gov/data/nccf/com/gfs/prod'
  # 
  RECORDS = { # Указываем записи с какими параметрами нас интересуют, подробнее о записях http://www.nco.ncep.noaa.gov/pmb/products/gfs/gfs_upgrade/gfs.t06z.pgrb2.0p25.f006.shtml
    apcp: ":APCP:surface:", # Влажность 
    tmp:   ":TMP:2 m above ground:", # Температура на высоте двух метров от поверхности земли
    dpt:  ":DPT:2 m above ground:", # Температура точки росы на высоте двух метров от поверхности земли
    gust: ":GUST:surface:" # Скорость ветра
  }

  attr_reader :filename,        # name of file in DIRECTORY 
              :date,            # date of forecast
              :hour_of_run,     # hour of gfs run
              :hour_of_forecast # hour of forecast

  # def self.clean_directory
  #   # all archive forecasts
  #   all_forecasts = Dir[File.join(DIRECTORY, "*")]
  #
  #   # archive forecasts on 6, 12, 18, 24 hours
  #   stored_forecasts = Dir[File.join(DIRECTORY, "*.f0{06,12,18,24}")]
  #
  #   (all_forecasts - stored_forecasts).each {|e| File.delete(e)}
  # end

  def initialize(date:, hour_of_run:, hour_of_forecast:)
    @date = date
    @hour_of_run = hour_of_run
    @hour_of_forecast = hour_of_forecast
    @filename = format("gfs_%04d_%02d_%02d_%02d.0p25.f%03d",  date.year, 
                                                              date.month, 
                                                              date.day, 
                                                              hour_of_run, 
                                                              hour_of_forecast)
  end

  def path
    File.join(DIRECTORY, @filename)
  end

  def to_s
    @filename
  end

  def fetched? 
    File.exist?(path)
  end

  def try_fetch(attemps: 1)
    attemps.times do 
      fetch
      return self if fetched?
    end

  rescue DownloadFailException => e
    self
  end

  def remove
    File.delete(path) if fetched?
  end  

  private

  def fetch
    return if fetched?

    ranges = fetch_ranges

    filtered_ranges = RECORDS.values.map { |v| ranges[v] }

    fetch_grib2(filtered_ranges)
  end

  def url
    subdir = format("gfs.%04d%02d%02d%02d", @date.year, @date.month, @date.day, @hour_of_run)
    filename = format("gfs.t%02dz.pgrb2.0p25.f%03d", @hour_of_run, @hour_of_forecast)
    format("%s/%s/%s", SERVER, subdir, filename)
  end

  def fetch_ranges
    begin
      response = Excon.get("#{url}.idx", :expects => 200)
    rescue Excon::Errors::Error => e
      raise DownloadFailException, "Download of '#{url}.idx' failed: #{e}"
    end

    lines = response.body.lines.map { |line| line.split(":") }

    lines.each_index.each_with_object({}) do |i, ranges|

      line = lines[i]

      next_line = lines[i + 1]

      key = ":#{line[3]}:#{line[4]}:"
     
      ranges[key] = [line[1].to_i]

      ranges[key] << next_line[1].to_i - 1 if next_line
    end
  end

  def fetch_grib2(ranges)
    streamer = lambda do |chunk, remaining, total|
      File.open(path, "ab") { |f| f.write(chunk) }
    end

    byte_ranges = ranges.map { |r| r.join("-") }.join(",")

    headers = { "Range" => "bytes=#{byte_ranges}" }

    begin
      Excon.get(url, :headers => headers, :response_block => streamer)
    rescue Excon::Errors::Error => e
      remove
      raise DownloadFailException, "Download of '#{url}' failed: #{e}"
    end
  end
end

# fcst =  Grib2FileHandler.new(date: Date.today - 1, 
#   hour_of_run: 0,
#   hour_of_forecast: 6
#   )

# puts !fcst.try_fetch(attemps: 5).nil?

# fcst.remove