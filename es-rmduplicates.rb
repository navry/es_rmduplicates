#!/usr/bin/env ruby
#encoding:utf-8
require 'rubygems'
require 'tire'
require 'yajl/json_gem'
require 'digest/sha1'

VERSION = '0.0.2'

STDOUT.sync = true

# PARAMETERS __START__

def printHelp
     STDERR.puts "Script to search and remove duplicites dates from ES.

Usage: #{__FILE__} <URL>/<INDEX> <ARGUMENTS>

Required arguments for configure JSON: 
       -i   json way to id (_id)
       -t   json way to type (_type)
       -d   json way to date (_source/date)
       -c   json way to duplicite content (_source/content)

Examples:
       #{__FILE__} http://localhost:9200/database -i _id -t _type \\
       -d _source/date -c _source/content > output.txt #(the best)

       #{__FILE__} http://localhost:9200/database -i _id -t _type \\
       -d _source/date -c _source/content 2>&1 /dev/null for silent mode

       For first example will be show counter on stderr\n"
end


if ARGV[0]
    if ARGV[0] =~ /^-(?:h|-?help)$/
        printHelp
        exit 0
    elsif ARGV[0] =~ /^-/
        puts "#{__FILE__}: illegal parameter,\n use #{__FILE__} -h for help"
        exit 1
    end
else
    printHelp
    exit 1
end


param,param_content,param_date,param_type,param_id = nil,nil,nil,nil,nil

while ARGV[0]
    case arg = ARGV.shift
    when '-h' then 
        printHelp
        exit 0
    when '-c' then
        param_content = ARGV.shift
    when '-d' then
        param_date = ARGV.shift
    when '-t' then
        param_type = ARGV.shift
    when '-i' then
        param_id = ARGV.shift
    else
        !param ? (param = arg) : 
            raise("Unexpected parameter '#{arg}'. Use '-h' for help.")
    end
end

urlS, index = '', ''


if param =~ %r{^http://(.*?)/(.*?)$}
    urlS.replace $1
    index.replace $2
else
    STDERR.puts "Url was not correct specified. Use '-h' for help"
    exit 1
end

if param_content == nil || param_date == nil ||
    param_type == nil || param_id == nil then
    STDERR.puts "Arguments were not fully specified. Use '-h' for help"
    exit 1
end

# PARAMETERS __END__

STDERR.print "Url:", urlS, " index:", index, "\n"

def retried_request method, url, data=nil
    """
    Function for sending query to ES
    """
    while true
        begin
            return data ?
                RestClient.send(method, url, data) :
                RestClient.send(method, url)
        rescue RestClient::ResourceNotFound # no point to retry
            puts "ERROR: no point to retry"
            return nil 
        rescue => e
            warn "\nRetrying #{method.to_s.upcase} ERROR: #{e.class} - #{e.message}"
        end 
    end 
end


def tm_len l
    """
    Function for converting time to legible form
    """
    t = []
    t.push l/86400; l %= 86400
    t.push l/3600;  l %= 3600
    t.push l/60;    l %= 60
    t.push l
    out = sprintf '%u', t.shift
    out = out == '0' ? '' : out + ' days, '
    out += sprintf('%u:%02u:%02u', *t)
    out
end


t, done = Time.now, 0 # COUNTER
data = Hash.new {|h,k| h[k]=[]}

shards = retried_request :get, "#{urlS}/#{index}/_count?q=*"
shards = Yajl::Parser.parse(shards)['_shards']['total'].to_i
scan = retried_request(:get, "#{urlS}/#{index}/_search" + "?search_type=scan&scroll=10m&size=#{1000 / shards}")
scan = Yajl::Parser.parse scan
scroll_id = scan['_scroll_id']
total = scan['hits']['total']

def trip (item, way)
    for each in way.split('/') do
        item = item[each]
    end

    return item
end



# LOAD DATABASE __START__

#while true do  
(1..2).each do #DEBUG for 2000 tests
    
    predata = retried_request(:get, "#{urlS}/_search/scroll?scroll=10m&scroll_id=#{scroll_id}") # Get
    predata = Yajl::Parser.parse predata
    break if predata['hits']['hits'].empty?
    scroll_id = predata['_scroll_id']

    predata['hits']['hits'].each{|doc|
        sha1 = Digest::SHA1.hexdigest(trip(doc,param_content)) # calculate hash
        data[sha1].push(doc) # Add new document to hash
        done += 1 # COUNTER

        #print data[sha1][0]['_source']['content'] # DEBUG
    }


    eta = total * (Time.now - t) / done # COUNTER
    STDERR.printf "  LOAD:  %u/%u (%.1f%%) done in %s, E.T.A.: %s.\r", # COUNTER
        done, total, 100.0 * done / total, tm_len(Time.now - t), t + eta # COUNTER
end

# LOAD DATABASE __END__

STDERR.puts # FORMATING OUTPUT

total = done # COUNTER
done = 0 # COUNTER
count = 0 # COUNTER

# REMOVE DUPLICITES __START__

data.each do |arr|
    if arr[1].length > 1 then # if is more documents on same hash
        count += 1 # DEBUG
        arr[1].sort_by! { |item| 
        # sort via Date/Time
            trip(item,param_date)
        }
        
        print count,"#",arr[1][0],"\n" # DEBUG

        arr[1].delete_at 0 # Remove first document (the oldes, don't remove from ES)

        arr[1].each do |item|
        # Remove remaining documents in array
            id = trip(item,param_id)
            date = trip(item,param_date)
            type = trip(item,param_type)

            print count,"#",arr[0],"#",id,"#",date,"\n" # DEBUG
            RestClient.send(:delete, "#{urlS}/#{index}/#{type}/#{id}")
        end
    end

    done += 1 # COUNER

    eta = total * (Time.now - t) / done # COUNTER
    STDERR.printf "DELETE:  %u/%u (%.1f%%) done in %s, E.T.A.: %s.\r", # COUNTER
        done, total, 100.0 * done / total, tm_len(Time.now - t), t + eta # COUNTER
end

# REMOVE DUPLICITES __END__

STDERR.puts # FORMATING OUTPUT
