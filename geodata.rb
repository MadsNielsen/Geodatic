#!/usr/bin/env ruby
# encoding: utf-8

require 'open-uri'
require 'mysql2'
require 'json'
require 'docopt'

def diff_parish
  #Connect to the database
  client = Mysql2::Client.new(:host => "localhost", :database => 'SAMMY', :username => "root", :password => "root")
  uri = URI.parse('http://geo.oiorest.dk/sogne.json')
  json = JSON.parse(uri.open.read)
  puts "Found #{json.length} authorities"
  
  sognenr_api = []
  
  json.each do |d|
    sognenr_api.push(d["nr"].to_i)
  end

  sognenr_db = []
  
  #Get sogne from our database
  client.query("SELECT * FROM SAM_AREA WHERE AREATYPEID = 'SOGN'").each do |row|
    sognenr_db.push(row["AREACODE"].to_i)
  end
  
  #Get the difference. Show those sogne that are not in the databse
  diff = (sognenr_api-sognenr_db)
  
  puts "There are #{diff.length} new authorities. Printing them here:"
  json.each do |sogn|
    if diff.include? sogn['nr'].to_i    
      puts "(#{sogn['nr']}) #{sogn['navn']}"
    end
  end
end

def reallocate_parishes_create_script(parish_range)
  puts "Invoking reallocate parish"
  #Connect to the database  
  kommune_id_upper = "kommuneid".swapcase
  road_id_upper = "roadid".swapcase
  house_id_upper = "houseid".swapcase
  sognenr_to_upper = "sognenr".swapcase
  sam_houseunits_upper = "sam_houseunits".swapcase
  threads_list = []
  
  script = "collect/update-script.sql"
  errorfile = "collect/errors.log"
  
  errors = []
  
  client = Mysql2::Client.new(:host => "localhost", :database => 'SAMMY', :username => "root", :password => "root")
  
  houseunits = []
  
  client.query("SELECT * from SAM_HOUSEUNITS WHERE SOGNENR >= #{parish_range.begin} AND SOGNENR <= #{parish_range.end}").each do |hu| 
    houseunits << hu
  end
  
  client.close
  
  puts houseunits.count

  parish_range.each do |parish_number|
    
    threads_list << Thread.new {
      update_cnt = 0
      updates = []
      error = "logs/error-#{parish_number}.log"   
      houseunits.select{ |hu| hu['SOGNENR'] == parish_number  }.each do |houseunit|
        y = "%0.15f" % houseunit['Y']
        x = "%0.15f" % houseunit['X']
        begin
          uri = URI.parse("http://dawa.aws.dk/adgangsadresser/reverse?x=#{x}&y=#{y}")          
          json = JSON.parse(uri.open.read)
          json_nr = json['sogn']['kode'].to_i
          
          if parish_number != json_nr 
            update_cnt = update_cnt + 1
            kommune_id = houseunit['KOMMUNEID']
            road_id = houseunit['ROADID']
            house_id = houseunit['HOUSEID']        
            val = "update #{sam_houseunits_upper} set #{sognenr_to_upper} = #{json_nr} where #{kommune_id_upper}=#{kommune_id} and #{road_id_upper}=#{road_id} and #{house_id_upper}=\'#{house_id}\';"
            puts val
            updates << val           
          end      
        rescue Exception => e
          e_msg = "#{e} - URI: #{uri}"
          errors << e_msg
        end
      end
      
      File.open(script,'a+') do |file| 
        file.puts "#Updated #{update_cnt} houses for parish #{parish_number}"
        updates.each do |upme| 
          file.puts upme
        end
      end
    }
    
  end
  
  threads_list.each do |t|
    t.join
  end
 
  File.open(script,'a+') do |file|
    puts "Writing update file." 
    file.puts "#Writing results for #{parish_range}"
    updates.each do |line|
      file.puts line
    end
  end
  
end

#Min sognenr = 7001
#Max sognenr = 9291
begin
  reallocate_parishes_create_script(7001..7100)
  reallocate_parishes_create_script(7101..7200)
  reallocate_parishes_create_script(7201..7300)
  reallocate_parishes_create_script(7301..7400)
  reallocate_parishes_create_script(7401..7500)
  reallocate_parishes_create_script(7501..7600)
  reallocate_parishes_create_script(7601..7700)
  reallocate_parishes_create_script(7701..7800)
  reallocate_parishes_create_script(7801..7900)
  reallocate_parishes_create_script(7901..8000)
  reallocate_parishes_create_script(8001..8100)
  reallocate_parishes_create_script(8101..8200)
  reallocate_parishes_create_script(8201..8300)
  reallocate_parishes_create_script(8301..8400)
  reallocate_parishes_create_script(8401..8500)
  reallocate_parishes_create_script(8501..8600)
  reallocate_parishes_create_script(8601..8700)
  reallocate_parishes_create_script(8701..8800)
  reallocate_parishes_create_script(8801..8900)
  reallocate_parishes_create_script(8901..9000)
  reallocate_parishes_create_script(9001..9100)
  reallocate_parishes_create_script(9101..9200)
  reallocate_parishes_create_script(9201..9291)
  reallocate_parishes_create_script(9999..9999)
end
   