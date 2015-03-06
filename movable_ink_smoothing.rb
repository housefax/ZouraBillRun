require 'rubygems'
require 'sinatra'
require 'json'
require 'csv'
require 'typhoeus'
require 'nokogiri'
require 'zipruby'
require 'date'
require 'time_diff'


def get_export_zip(query)
  #############################
  #### Start of login call ####
  #############################
  puts '=====> Export start'
  request = Nokogiri::XML::Builder.new do |xml|
    xml['SOAP-ENV'].Envelope('xmlns:SOAP-ENV' =>"http://schemas.xmlsoap.org/soap/envelope/", 'xmlns:api' => "http://api.zuora.com/" ) do        
      xml['SOAP-ENV'].Header
      xml['SOAP-ENV'].Body do
        xml['api'].login do
          xml['api'].username @username
          xml['api'].password @password
        end
      end
    end
  end
  response_query = Typhoeus::Request.post(@api_url, :body => request.to_xml, :headers => {'Content-Type' => "text/xml; charset=utf-8"})
  output_xml = Nokogiri::XML(response_query.body )

  return 'Login Unsuccessful : ' +  output_xml.xpath('//fns:FaultMessage', 'fns' =>'http://fault.api.zuora.com/').text if output_xml.xpath('//ns1:Session', 'ns1' =>'http://api.zuora.com/').text == ""
  session = output_xml.xpath('//ns1:Session', 'ns1' =>'http://api.zuora.com/').text

  ############################
  ##### End of login call ####
  ############################    

  request = Nokogiri::XML::Builder.new do |xml|
    xml['SOAP-ENV'].Envelope('xmlns:SOAP-ENV' => "http://schemas.xmlsoap.org/soap/envelope/", 'xmlns:ns2' => "http://object.api.zuora.com/", 'xmlns:xsi' => "http://www.w3.org/2001/XMLSchema-instance", 'xmlns:ns1' => "http://api.zuora.com/") do
      xml['SOAP-ENV'].Header do
        xml['ns1'].SessionHeader do
          xml['ns1'].session session
        end
      end
      xml['SOAP-ENV'].Body do
        xml['ns1'].create do
          xml['ns1'].zObjects('xsi:type' => "ns2:Export") do
            xml['ns2'].Format 'csv'
            xml['ns2'].Zip 'true'
            xml['ns2'].Name 'googman'
            xml['ns2'].Query query 
          end
        end  
      end
    end
  end
  response_query = Typhoeus::Request.post(@api_url, :body => request.to_xml, :headers => {'Content-Type' => "text/xml; charset=utf-8"})
  output_xml = Nokogiri::XML(response_query.body)

  return 'Export Creation Unsuccessful : ' + output_xml.xpath('//ns1:Message', 'ns1' =>'http://api.zuora.com/').text if  output_xml.xpath('//ns1:Success', 'ns1' =>'http://api.zuora.com/').text != "true"
  id = output_xml.xpath('//ns1:Id', 'ns1' =>'http://api.zuora.com/').text

  confirmRequest = Nokogiri::XML::Builder.new do |xml|
    xml['SOAP-ENV'].Envelope('xmlns:SOAP-ENV' => "http://schemas.xmlsoap.org/soap/envelope/", 'xmlns:ns2' => "http://object.api.zuora.com/", 'xmlns:xsi' => "http://www.w3.org/2001/XMLSchema-instance", 'xmlns:ns1' => "http://api.zuora.com/") do
      xml['SOAP-ENV'].Header do
        xml['ns1'].SessionHeader do
          xml['ns1'].session session
        end
      end
      xml['SOAP-ENV'].Body do
        xml['ns1'].query do
          xml['ns1'].queryString "SELECT  Id, CreatedById, CreatedDate, Encrypted, FileId, Format, Name, Query, Size, Status, StatusReason, UpdatedById, UpdatedDate, Zip From Export where id='" + id + "'"
        end  
      end
    end
  end

  result = 'Waiting'
  while result != "Completed"
    sleep 3
    response_query = Typhoeus::Request.post(@api_url, :body => confirmRequest.to_xml, :headers => {'Content-Type' => "text/xml; charset=utf-8"})
    output_xml = Nokogiri::XML(response_query.body)

    result = output_xml.xpath('//ns2:Status',  'ns2' =>'http://object.api.zuora.com/').text
    puts "=====> Export Status: " + result
    
    return 'Export Creation Unsuccessful : ' + output_xml.xpath('//ns1:Message', 'ns1' =>'http://api.zuora.com/').text if result == "Failed"
  end
  file_id = output_xml.xpath('//ns2:FileId',  'ns2' =>'http://object.api.zuora.com/').text
  response_query = Typhoeus::Request.get(@file_url + file_id, params: {"file-id" => file_id}, headers: {"Authorization" => "ZSession " + session})

  puts '=====> Export finished'
  return response_query.body
end

def update_batch(batch)
  #############################
  #### Start of login call ####
  #############################

  request = Nokogiri::XML::Builder.new do |xml|
    xml['SOAP-ENV'].Envelope('xmlns:SOAP-ENV' =>"http://schemas.xmlsoap.org/soap/envelope/", 'xmlns:api' => "http://api.zuora.com/" ) do        
      xml['SOAP-ENV'].Header
      xml['SOAP-ENV'].Body do
        xml['api'].login do
          xml['api'].username @username
          xml['api'].password @password
        end
      end
    end
  end
  response_query = Typhoeus::Request.post(@api_url, :body => request.to_xml, :headers => {'Content-Type' => "text/xml; charset=utf-8"})
  output_xml = Nokogiri::XML(response_query.body )

  return 'Login Unsuccessful : ' +  output_xml.xpath('//fns:FaultMessage', 'fns' =>'http://fault.api.zuora.com/').text if output_xml.xpath('//ns1:Session', 'ns1' =>'http://api.zuora.com/').text == ""
  session = output_xml.xpath('//ns1:Session', 'ns1' =>'http://api.zuora.com/').text

  request = Nokogiri::XML::Builder.new do |xml|
    xml['SOAP-ENV'].Envelope('xmlns:SOAP-ENV' => "http://schemas.xmlsoap.org/soap/envelope/", 'xmlns:ns2' => "http://object.api.zuora.com/", 'xmlns:xsi' => "http://www.w3.org/2001/XMLSchema-instance", 'xmlns:ns1' => "http://api.zuora.com/") do
      xml['SOAP-ENV'].Header do
        xml['ns1'].SessionHeader do
          xml['ns1'].session session
        end
      end
      xml['SOAP-ENV'].Body do
        xml['ns1'].update do
          batch.each do |rpc|
            xml['ns1'].zObjects('xsi:type' => "ns2:RatePlanCharge") do
              xml['ns2'].Id rpc[@ratePlanChargeKeys.index("RatePlanCharge.Id")]
              xml['ns2'].TotalUnitsUsed__c rpc.last[0]
              xml['ns2'].TotalUnitsAvailable__c rpc.last[1]
            end
          end
        end  
      end
    end
  end

  response_query = Typhoeus::Request.post(@api_url, :body => request.to_xml, :headers => {'Content-Type' => "text/xml; charset=utf-8"})
  output_xml = Nokogiri::XML(response_query.body)
end


before do
  #@api_url =  "https://www.zuora.com/apps/services/a/62.0" 
  #@file_url =  "https://www.zuora.com/apps/api/file/"
  #@username = "adam@z-movableink.com"
  #@password =  "Char!ie14"
  @api_url =  "https://apisandbox.zuora.com/apps/services/a/62.0" 
  @file_url =  "https://apisandbox.zuora.com/apps/api/file/" 
  @username = "taylor.medford@movableink.sandbox.com"
  @password =  "64ybpVToUwci90"
end

# Handle GET-request
get "/" do
  erb :index
end   

get "/download/:file" do 
  send_file('' + params[:file] , type: "application/csv", :filename => params[:file])
end  
    

get "/callout" do 
    # request.body              # request body sent by the client (see below)
    # request.scheme            # "http"
    # request.script_name       # "/example"
    # request.path_info         # "/foo"
    # request.port              # 80
    # request.request_method    # "GET"
    # request.query_string      # ""
    # request.content_length    # length of request.body
    # request.media_type        # media type of request.body
    # request.host              # "example.com"
    # request.get?              # true (similar methods for other verbs)
    # request.form_data?        # false
    # request["SOME_HEADER"]    # value of SOME_HEADER header
    # request.referer           # the referer of the client or '/'
    # request.user_agent        # user agent (used by :agent condition)
    # request.cookies           # hash of browser cookies
    # request.xhr?              # is this an ajax request?
    # request.url               # "http://example.com/example/foo"
    # request.path              # "/example/foo"
    # request.ip                # client IP address
    # request.secure?           # false
    puts request.inspect
    return  request.inspect
    #return request.env 
end  

# Handle POST-request 
post "/" do 
  puts '=> Start processing'

  #############################################
  #### Subscription Id map to accountNumber ### 
  #############################################
  puts '===> Get data source export of subscription with accounts'
  subscriptionAccountQuery = "Select Subscription.Id, Account.AccountNumber from Subscription where Subscription.Status = 'Active'"
  subscriptionAccountZipbody = get_export_zip(subscriptionAccountQuery)

  puts '===> Construct subscription to account map given export data'
  subscriptionAccountHash = Hash.new()
  Zip::Archive.open_buffer(subscriptionAccountZipbody) do |ar|
     ar.fopen(0) do |zf|
        open(zf.name, 'wb') do |f|
          CSV.parse(zf.read) do |row|
            subscriptionAccountHash[row[0]] = row[1]
          end
        end
     end
  end

  ############################################################
  #### Datasource Get Active rateplancharges with rollover ### 
  ############################################################
  puts '===> Get data source export of rateplancharges with rollover'
  ratePlanChargeTierQuery = "Select RatePlanChargeTier.IncludedUnits, RatePlanChargeTier.Tier, Subscription.Id, RatePlanCharge.Id, RatePlanCharge.ProcessedThroughDate, RatePlanCharge.BillingPeriod, RatePlanCharge.ChargeModel, RatePlanCharge.ChargeType, RatePlanCharge.EffectiveStartDate, RatePlanCharge.NumberOfPeriods, RatePlanCharge.OverageCalculationOption, RatePlanCharge.UOM, RatePlanCharge.BillCycleDay, ProductRatePlanCharge.SmoothingModel from RatePlanChargeTier where Subscription.Status = 'Active' and RatePlanCharge.ChargeType = 'usage' and ProductRatePlanCharge.SmoothingModel ='RollingWindow' and RatePlanChargeTier.Tier = 1"
  ratePlanChargeTierZipbody = get_export_zip(ratePlanChargeTierQuery)
  
  puts '===> Construct rateplancharge map with given export data'
  ratePlanCharges = []
  @ratePlanChargeKeys = []
  ratePlanChargeHash = Hash.new()
  oldestStartDate = nil
  longestPeriod = 0
  Zip::Archive.open_buffer(ratePlanChargeTierZipbody) do |ar|
     ar.fopen(0) do |zf|
        open(zf.name, 'wb') do |f|
          CSV.parse(zf.read) do |row|
            if @ratePlanChargeKeys == []
              @ratePlanChargeKeys.push(*row)
            else
              accountNumber = subscriptionAccountHash[row[@ratePlanChargeKeys.index("Subscription.Id")]]
              ratePlanChargeHash[accountNumber] = Hash.new() if ratePlanChargeHash[accountNumber] == nil
              ratePlanChargeHash[accountNumber][row[@ratePlanChargeKeys.index("RatePlanCharge.Id")]] = row

              ratePlanCharges.push(row)
              effectiveStartDate = row[@ratePlanChargeKeys.index("RatePlanCharge.EffectiveStartDate")] == nil ? false :  row[@ratePlanChargeKeys.index("RatePlanCharge.EffectiveStartDate")]
              effectiveStartDate = effectiveStartDate != false && effectiveStartDate.length == 24 ? effectiveStartDate[0..18] + "-0800" : effectiveStartDate  
              effectiveStartDate = effectiveStartDate != false && effectiveStartDate.length == 10 ? effectiveStartDate + "T00:00:00-0800" : effectiveStartDate  
              effectiveStartDate = effectiveStartDate != false ? DateTime.iso8601(effectiveStartDate) : oldestStartDate
              oldestStartDate = effectiveStartDate if oldestStartDate == nil || oldestStartDate == false
              oldestStartDate = oldestStartDate > effectiveStartDate ? effectiveStartDate  :  oldestStartDate

              case row[@ratePlanChargeKeys.index("RatePlanCharge.BillingPeriod")]
              when "Month"
                months = row[@ratePlanChargeKeys.index("RatePlanCharge.NumberOfPeriods")].to_i 
              when "Quarter"
                months = row[@ratePlanChargeKeys.index("RatePlanCharge.NumberOfPeriods")].to_i * 4
              when "Annual"
                months = row[@ratePlanChargeKeys.index("RatePlanCharge.NumberOfPeriods")].to_i * 12
              when "Semi-Annual"
                months = row[@ratePlanChargeKeys.index("RatePlanCharge.NumberOfPeriods")].to_i * 6
              else
                months = row[@ratePlanChargeKeys.index("RatePlanCharge.NumberOfPeriods")].to_i
              end
              longestPeriod = longestPeriod < months ? months :  longestPeriod
            end
          end
        end
     end
  end
  ##Used to determine if we need to pull usage from begining of oldest start date or from maximum possible billing period search range.
  billingPeriodSearchRange =  longestPeriod
  usageEndpoint = ((Time.now.year * 12 + Time.now.month) - (oldestStartDate.year * 12 + oldestStartDate.month)).abs > longestPeriod ?  Time.now - billingPeriodSearchRange.months : oldestStartDate

  ########################################################################################
  #### Datasource Get Usages as far back to determine when last overage if one occured ### 
  ########################################################################################
  puts '===> Get data source export of usage'
  usageQuery = "Select Usage.AccountNumber, Usage.Quantity, Usage.StartDateTime, Usage.UOM from Usage where Usage.StartDateTime >= '#{usageEndpoint.strftime('%m/%d/%Y')}' "
  usagezipbody = get_export_zip(usageQuery)

  puts '===> Construct usage map given export data'
  usageHash = Hash.new()
  usages = []
  usageKeys = []
  Zip::Archive.open_buffer(usagezipbody) do |ar|
     ar.fopen(0) do |zf|
        open(zf.name, 'wb') do |f|
          CSV.parse(zf.read) do |row|
            if usageKeys == []
              usageKeys.push(*row)
            else
              usages.push(row)
              usageHash[row[usageKeys.index("Usage.AccountNumber")]] = Hash.new() if usageHash[row[usageKeys.index("Usage.AccountNumber")]] == nil
              usageHash[row[usageKeys.index("Usage.AccountNumber")]][row[usageKeys.index("Usage.UOM")]] = Hash.new() if usageHash[row[usageKeys.index("Usage.AccountNumber")]][row[usageKeys.index("Usage.UOM")]] == nil
              time = DateTime.parse(row[usageKeys.index("Usage.StartDateTime")]).to_i
              usageHash[row[usageKeys.index("Usage.AccountNumber")]][row[usageKeys.index("Usage.UOM")]][time] = 0.00 if usageHash[row[usageKeys.index("Usage.AccountNumber")]][row[usageKeys.index("Usage.UOM")]][time] == nil
              usageHash[row[usageKeys.index("Usage.AccountNumber")]][row[usageKeys.index("Usage.UOM")]][time] = usageHash[row[usageKeys.index("Usage.AccountNumber")]][row[usageKeys.index("Usage.UOM")]][time] + row[usageKeys.index("Usage.Quantity")].to_f
            end
          end
        end
     end
  end

  #################################################################################
  #### Go through SubscriptionRatePlanCharges and Figure current rolling window ### 
  #################################################################################
  puts '===> Loop through rateplancharges and calculate rolling usage'

  ratePlanCharges.each_with_index do | rateplancharge, i|
    #puts '--------------- : i'
    subid = rateplancharge[@ratePlanChargeKeys.index("Subscription.Id")]
    accountNumber = subscriptionAccountHash[subid]
    unitOfMeasure = rateplancharge[@ratePlanChargeKeys.index("RatePlanCharge.UOM")]
    rateplanChargeId = rateplancharge[@ratePlanChargeKeys.index("RatePlanCharge.Id")]
    includedUnits = rateplancharge[@ratePlanChargeKeys.index("RatePlanChargeTier.IncludedUnits")].to_f
    billing_period = rateplancharge[@ratePlanChargeKeys.index("RatePlanCharge.BillingPeriod")]
    @numberPeriods = rateplancharge[@ratePlanChargeKeys.index("RatePlanCharge.NumberOfPeriods")].to_i
    @totalAllowableUnits = includedUnits * @numberPeriods
    usage = usageHash[accountNumber] == nil ? Hash.new() : usageHash[accountNumber]
    usage = usage[unitOfMeasure] == nil ? Hash.new() : usage[unitOfMeasure]
  
    effectiveStartDate = rateplancharge[@ratePlanChargeKeys.index("RatePlanCharge.EffectiveStartDate")] == nil ? false :  rateplancharge[@ratePlanChargeKeys.index("RatePlanCharge.EffectiveStartDate")]
    effectiveStartDate = effectiveStartDate != false && effectiveStartDate.length == 24 ? effectiveStartDate[0..18] + "-0800" : effectiveStartDate  
    effectiveStartDate = effectiveStartDate != false && effectiveStartDate.length == 10 ? effectiveStartDate + "T00:00:00-0800" : effectiveStartDate  
    effectiveStartDate = effectiveStartDate != false ? DateTime.iso8601(effectiveStartDate) : effectiveStartDate

    processedThroughDate = rateplancharge[@ratePlanChargeKeys.index("RatePlanCharge.ProcessedThroughDate")] == nil ? false :  rateplancharge[@ratePlanChargeKeys.index("RatePlanCharge.ProcessedThroughDate")]
    processedThroughDate = processedThroughDate != false && processedThroughDate.length == 24 ? processedThroughDate[0..18] + "-0800" : processedThroughDate  
    processedThroughDate = processedThroughDate != false && processedThroughDate.length == 10 ? processedThroughDate + "T00:00:00-0800" : processedThroughDate  
    processedThroughDate = processedThroughDate != false ? DateTime.iso8601(processedThroughDate) : processedThroughDate

    ##Build Usage History
    cycles = processedThroughDate == false ? 0 : ((processedThroughDate.year * 12 + processedThroughDate.month) - (effectiveStartDate.year * 12 + effectiveStartDate.month))
    usageCycle = []
    cycles = cycles-1 if cycles !=0

    (0..cycles).each do |period|
      case billing_period
      when "Month"
        startTime = effectiveStartDate + (period).months 
        endTime = effectiveStartDate + (period+1).months-1.minute
      when "Quarter"
        startTime = effectiveStartDate + (period*4).months 
        endTime = effectiveStartDate + ((period*4)+4).months-1.minute
      when "Annual"
        startTime = effectiveStartDate + (period).years 
        endTime = effectiveStartDate + (period+1).years-1.minute
      when "Semi-Annual"
        startTime = effectiveStartDate + (period*6).months 
        endTime = effectiveStartDate + ((period*6)+6).months-1.minute
      else
        startTime = effectiveStartDate + (period).months 
        endTime = effectiveStartDate + (period+1).months-1.minute
      end

      periodUsageHash = usage.select {|key,value| key.between?(startTime.to_i, endTime.to_i) }
      periodUsage = periodUsageHash.values.reduce(:+) 
      periodUsage = periodUsage == nil ? 0.00 : periodUsage

      #cycle_overage = overage.select {|key,value| key.between?(startTime.to_i, endTime.to_i) }.size > 0 ? true : false
      end_smooth = usageCycle.map { |v,k| if v['smooth_end'] == true then true else false end }.split(true).last.size == @numberPeriods-1 ? true : false
      sequence = usageCycle.map { |v,k| if v['smooth_end'] == false then v['usage']  else true end }.split(true).last.reduce(:+)

      #sequence = sequence.size>@numberPeriods-1 ? sequence[-@numberPeriods+1,@numberPeriods-1].reject(&:blank?).reduce(:+) : sequence.reject(&:blank?).reduce(:+)
      running_window_usage = [periodUsage, sequence].reject(&:blank?).reduce(:+)

      usageCycle[period] = {'startTime' => startTime, 'endTime' => endTime, 'usage' => periodUsage, 'smooth_end' =>  end_smooth, 'running_usage' => running_window_usage, 'total_allowed' => @totalAllowableUnits}
    end

    ##usageCycle.each do |x| ; puts x.inspect ; end;nil
    usageCycle.reverse! 
    rateplancharge.push([usageCycle.first['running_usage'] ,usageCycle.first['total_allowed'], usageCycle  ])
  end


  #############################################
  #### Datasource on invoice item to update ### 
  #############################################
  puts '===> Get data source export of invoice items'
  invoiceItemQuery = "select InvoiceItem.ChargeAmount, InvoiceItem.Id, InvoiceItem.Quantity, InvoiceItem.ServiceEndDate, InvoiceItem.ServiceStartDate, InvoiceItem.SubscriptionId, InvoiceItem.TotalAvailUnits__c, InvoiceItem.TotalUsedUnits__c, InvoiceItem.UOM, InvoiceItem.UnitPrice, Invoice.Id, Invoice.Status, ProductRatePlanCharge.SmoothingModel, RatePlanCharge.ChargeType from InvoiceItem where (((Invoice.Status like 'Draft' and InvoiceItem.ProcessingType = 'Charge') and RatePlanCharge.ChargeType = 'usage') and ProductRatePlanCharge.SmoothingModel = 'RollingWindow')"  
  invoiceitemzipbody = get_export_zip(invoiceItemQuery)

  puts '===> Construct invvoice item map given export data'
  invoiceItemsHash = Hash.new()
  invoiceItems = []
  invoiceItemsKeys = []
  Zip::Archive.open_buffer(invoiceitemzipbody) do |ar|
     ar.fopen(0) do |zf|
        open(zf.name, 'wb') do |f|
          CSV.parse(zf.read) do |row|
            if invoiceItemsKeys == []
              invoiceItemsKeys.push(*row)
            else
              invoiceItems.push(row)
            end
          end
        end
     end
  end





  puts '===> Update RatePlanCharge custom fields for running usage and total allowable usage'
  APP_ROOT = File.dirname(__FILE__)
  file_name = 'output-' + Time.now.to_i.to_s + '.csv'
  output_file = File.join(APP_ROOT, file_name ) 

  CSV.open(output_file, "w") do |csv|
    csv << ['SubscriptionId','RatePaneChargeId', 'Success', 'Message', 'Debug']
    ratePlanCharges.each_slice(50) do | batch |
      response = update_batch(batch).xpath('//ns1:result', 'ns1' =>'http://api.zuora.com/')
      response.each_with_index do |call, i|
        status = call.xpath('./ns1:Success', 'ns1' =>'http://api.zuora.com/').text
        #id_recieved =  call.xpath('./ns1:Id', 'ns1' =>'http://api.zuora.com/').text
        message = status == 'false' ? call.xpath('./*/ns1:Code', 'ns1' =>'http://api.zuora.com/').text + ': ' + call.xpath('./*/ns1:Message', 'ns1' =>'http://api.zuora.com/').text : nil
        id_sent = batch[i][@ratePlanChargeKeys.index("RatePlanCharge.Id")]
        sub_id = batch[i][@ratePlanChargeKeys.index("Subscription.Id")]
        debug =  batch[i].last[2]  
        csv << [sub_id, id_sent, status, message, debug] 
      end  
    end
  end


  puts '=> Finished '
  File.delete('Subscription.csv'); File.delete('Usage.csv') ; File.delete('RatePlanChargeTier.csv')

  @download_last = true
  erb :index    
end 

