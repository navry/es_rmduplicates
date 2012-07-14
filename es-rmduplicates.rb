#!/usr/bin/env ruby
#encoding:utf-8
require 'rubygems'
require 'tire'
require 'yajl/json_gem'
require 'digest/sha1'

VERSION = '0.0.2'

STDOUT.sync = true

# PARAMETERS __START__

def help
     puts "Script to search and remove duplicites dates from ES.

Usage:
       #{__FILE__} [http://source_url/]<index>

Examples:
       #{__FILE__} http://localhost:9200/database > output.txt #(the best)
       #{__FILE__} http://localhost:9200/database 2>&1 /dev/null

       For first example will be show counter on stderr\n"
end


if ARGV[0]
    if ARGV[0] =~ /^-(?:h|-?help)$/
        help
        exit 0
    elsif ARGV[0] =~ /^-/
        puts "#{__FILE__}: illegal parameter,\n use #{__FILE__} -h for help"
        exit 1
    end
else
    help
    exit 1
end


param = nil

while ARGV[0]
    case arg = ARGV.shift
    when '-h' then help=true
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
    help
    exit 1
end

# PARAMETERS __END__

STDERR.print "Url:", urlS, " index:", index, "\n"

def retried_request method, url, data=nil
    """
    Function for sending requirement on ES
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
    Function for converd time to legible form
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


# LOAD DATABASE __START__

while true do  
#(1..2).each do #DEBUG
    
    predata = retried_request(:get, "#{urlS}/_search/scroll?scroll=10m&scroll_id=#{scroll_id}") # Get
    predata = Yajl::Parser.parse predata
    break if predata['hits']['hits'].empty?
    scroll_id = predata['_scroll_id']

    predata['hits']['hits'].each{|doc|
        
        sha1 = Digest::SHA1.hexdigest(doc['_source']['content']) # calculate hash
        data[sha1].push(doc) # Add new document to hash
        data[sha1][0]['_source']['content'] = nil # Delete countent dates (save ram)

        done += 1 # COUNTER
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
            item['_source']['date']
        }
        
        print count,"#",arr[1][0],"\n" # DEBUG

        arr[1].delete_at 0 # Remove first document (the oldes, don't remove from ES)

        arr[1].each do |item|
        # Remove remaining documents in array
            print count,"#",arr[0],"#",item['_id'],"#",item['_source']['date'],"\n" # DEBUG
            RestClient.send(:delete, "#{urlS}/#{index}/#{item['_type']}/#{item['_id']}")
        end
    end

    done += 1 # COUNER

    eta = total * (Time.now - t) / done # COUNTER
    STDERR.printf "  DELETE  %u/%u (%.1f%%) done in %s, E.T.A.: %s.\r", # COUNTER
        done, total, 100.0 * done / total, tm_len(Time.now - t), t + eta # COUNTER
end

# REMOVE DUPLICITES __END__

STDERR.puts # FORMATING OUTPUT
