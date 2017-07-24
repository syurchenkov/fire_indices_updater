require 'sequel'

module FireIndexDatabase
  DATABASEPATH = "./database/database.db"

  module_function

  def coords_to_id(latitude, longitude)
    id = (latitude + 90) * 4 * 360 * 4 + (longitude * 4)
  end

  def create
    db = Sequel.sqlite(DATABASEPATH)

    db.create_table :fire_indices do 
      primary_key :id
      Float :longitude
      Float :latitude
      Integer :nesterov1
      Integer :nesterov2
      Integer :nesterov3
      Integer :nesterov4
      Integer :nesterov5
      Integer :nesterov6
      Integer :nesterov7
    end

    fire_indices = db[:fire_indices]

    (-90..90).step(0.25) do |latitude|
      puts "Current latitude: #{latitude}"

      records = []

      (0...360).step(0.25) do |longitude|
        id = coords_to_id(latitude, longitude)

        records << [id, longitude, latitude, 0, 0, 0, 0, 0, 0, 0]
      end

      fire_indices.import(
        [:id, :longitude, :latitude, :nesterov1, :nesterov2, :nesterov3, :nesterov4, :nesterov5, :nesterov6, :nesterov7],
        records)
    end
  end
end

if __FILE__ == $0
  FireIndexDatabase.create
end
