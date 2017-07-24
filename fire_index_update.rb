require 'rufus-scheduler'
require './grib2_file_handler.rb'
require './grib2_txt_handler.rb'
require './fire_index_database.rb'
require './nesterov_fire_index.rb'

module FireIndexUpdate
  extend FireIndexDatabase

  HOUR_OF_FORECAST = [6, 12, 18, 24]

  DAYS_OF_FORECAST = 7

  module_function

  def update

    scheduler = Rufus::Scheduler.singleton

    scheduler.every('24h', {:overlap => false, :first => :now}) do 
      grib2_files = download_grib2_files

      if grib2_files.all? { |gf| gf.fetched? }
        begin
          
          txt_files = grib2_files.map{|f| Grib2TxtHandler.convert(f) }

        rescue Grib2TxtHandler::ConvertationFailException => e
          puts "Grib2 to txt file error"
          
          puts e

          return 
        end

        fire_index_computation(txt_files)

      else 
        not_fetched_files = grib2_files.select {|gf| not gf.fetched? }

        puts "Cannot fetch this list of files:\n"

        puts not_fetched_file      
      end 
    end
  end

  def download_grib2_files
    date = Date.today - 1

    grib2_files = (0...DAYS_OF_FORECAST).map do |day|
      HOUR_OF_FORECAST.map do |hour|
        Grib2FileHandler.new(date: date, hour_of_run: 0, hour_of_forecast: day * 24 + hour).
          try_fetch(attemps: 5)
      end
    end

    grib2_files.flatten!
  end

  def fire_index_computation(txt_files)
    db = Sequel.sqlite(FireIndexDatabase::DATABASEPATH)

    fire_indices = db[:fire_indices]

    puts

    (-90..90).step(0.25) do |latitude|
      #puts "Current latitude: #{latitude}"

      records = []

      (0...360).step(0.25) do |longitude|
        #print "\r#{latitude}, #{longitude}                     "
        h = extract_data_txt(txt_files)

        indices = NesterovFireIndex.compute(apcp: h[:apcp], tmp: h[:tmp], dpt: h[:dpt])

        id = coords_to_id(latitude, longitude)

        records << [id, indices]
      end

      db.transaction do
        records.each do |r|
          fire_indices.where(:id => r[0]).update(
              :nesterov1 => r[1][0],
              :nesterov2 => r[1][1],
              :nesterov3 => r[1][2],
              :nesterov4 => r[1][3],
              :nesterov5 => r[1][4],
              :nesterov6 => r[1][5],
              :nesterov7 => r[1][6]
            )
        end
      end
    end
  end

  def extract_data_txt(grib2_txt)
    apcp = []
    gust = []
    tmp  = []
    dpt  = []

    grib2_txt.each do |t|
      apcp << t['APCP:surface'].get_value
      gust << t['GUST:surface'].get_value
      tmp  << t['TMP:2 m above ground'].get_value
      dpt  << t['DPT:2 m above ground'].get_value
    end

    return {apcp: apcp, gust: gust, tmp: tmp, dpt: dpt}
  end

end

if __FILE__ == $0
  begin

    FireIndexUpdate.update

    puts "\nUpdate finish"

    Rufus::Scheduler.singleton.join

  rescue SystemExit, Interrupt
    Rufus::Scheduler.singleton.shutdown(:wait)

    puts "\nUpdater stopped"

  rescue Exception => e
    puts e
  end
end



