require 'rubygems'
require 'sinatra'
require 'json'
require 'csv'
require 'typhoeus'
require 'nokogiri'
require 'zipruby'
require 'date'
require 'pony'

def zuora_login()
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

  raise 'Login Unsuccessful : ' +  output_xml.xpath('//fns:FaultMessage', 'fns' =>'http://fault.api.zuora.com/').text if output_xml.xpath('//ns1:Session', 'ns1' =>'http://api.zuora.com/').text == ""
  return  output_xml.xpath('//ns1:Session', 'ns1' =>'http://api.zuora.com/').text
end

def get_export_zip(query)
  session = zuora_login()

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
  raise 'Export Creation Unsuccessful : ' + output_xml.xpath('//ns1:Message', 'ns1' =>'http://api.zuora.com/').text if  output_xml.xpath('//ns1:Success', 'ns1' =>'http://api.zuora.com/').text != "true"
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
    
    raise 'Export Creation Unsuccessful : ' + output_xml.xpath('//ns1:Message', 'ns1' =>'http://api.zuora.com/').text if result == "Failed"
  end
  file_id = output_xml.xpath('//ns2:FileId',  'ns2' =>'http://object.api.zuora.com/').text
  response_query = Typhoeus::Request.get(@file_url + file_id, params: {"file-id" => file_id}, headers: {"Authorization" => "ZSession " + session})

  puts '=====> Export finished'
  return response_query.body
end

def soap_call(ns1: 'ns1', ns2: 'ns2', **keyword_args)
  xml = Nokogiri::XML::Builder.new do |xml|
    xml['SOAP-ENV'].Envelope('xmlns:SOAP-ENV' => "http://schemas.xmlsoap.org/soap/envelope/", 
                             "xmlns:#{ns2}" => "http://object.api.zuora.com/", 
                             'xmlns:xsi' => "http://www.w3.org/2001/XMLSchema-instance", 
                             'xmlns:api' => "http://api.zuora.com/", 
                             "xmlns:#{ns1}" => "http://api.zuora.com/") do
      xml['SOAP-ENV'].Header do
        xml["#{ns1}"].SessionHeader do
          xml["#{ns1}"].session zuora_login()
        end
      end
      xml['SOAP-ENV'].Body do
        yield xml, keyword_args
      end
    end
  end

  input_xml = Nokogiri::XML(xml.to_xml(:save_with => Nokogiri::XML::Node::SaveOptions::AS_XML | Nokogiri::XML::Node::SaveOptions::NO_DECLARATION).strip)
  input_xml.xpath('//ns1:session', 'ns1' =>'http://api.zuora.com/').children.remove
  response = Typhoeus::Request.post(@api_url, :body => xml.doc.to_xml, :headers => {'Content-Type' => "text/xml; charset=utf-8"})
  output_xml = Nokogiri::XML(response.body)

  raise "#{output_xml.xpath('//fns:FaultCode', 'fns' =>'http://fault.api.zuora.com/').text}::#{output_xml.xpath('//fns:FaultMessage', 'fns' =>'http://fault.api.zuora.com/').text}"  if !output_xml.xpath('//fns:FaultCode', 'fns' =>'http://fault.api.zuora.com/').text == ""
  raise "#{output_xml.xpath('//faultcode').text}::#{output_xml.xpath('//faultstring').text}"  if !output_xml.xpath('//faultcode').text == ""

  return [output_xml, input_xml]
end


#use Rack::Auth::Basic, "Restricted Area" do |username, password|
#  username == 'test@test.com' and password == 'test123'
#end

before do
  @api_url  = "https://apisandbox.zuora.com/apps/services/a/62.0" 
  @file_url = "https://apisandbox.zuora.com/apps/api/file/" 
  @username = "taylor.medford@zuora.com"
  @password = "%J52izEkvZjy"
end

# Handle GET-request
get "/" do
  erb :index
end   

get "/download/:file" do 
  send_file('' + params[:file] , type: "application/csv", :filename => params[:file])
end  
    
post "/recalculate" do 
  begin
    raise "Not Implemented"
  rescue => ex
    @error = ex.message
  end
  erb :index  
end

post "/callout" do 
  begin
    puts '=> Start processing'
    @BillRunId = params[:BillingRunId].nil? ? nil : params[:BillingRunId]

    #raise "Usage Processing can only be triggerd by a callout" if @BillRunId.nil?
    #######################################################################################################################################################
    ################################################################### START Pulling Data ################################################################
    #######################################################################################################################################################

    #############################################
    #### Datasource on invoice item to update ### 
    #############################################
    puts '===> Get data source export of invoice items'
    

    #@BillRunId = '2c92c0f955a0b5b80155a8af44a82f90'

    invoiceitemzipbody = get_export_zip("select RatePlanCharge.Id, RatePlanCharge.Quantity, RatePlanCharge.Tracking__c, RatePlanCharge.Available__c, ProductRatePlanCharge.SmoothingModel from InvoiceItem where (ProductRatePlanCharge.ChargeType = 'Usage' and Invoice.SourceId = '#{@BillRunId}')" )

    invoiceItemsKeys = []
    ratePlanChargeHash =  Hash.new()
    Zip::Archive.open_buffer(invoiceitemzipbody) do |ar|
      ar.fopen(0) do |zf|
        open(zf.name, 'wb') do |f|
          CSV.parse(zf.read) do |row|
            if invoiceItemsKeys == []
              invoiceItemsKeys.push(*row)
            else
              ratePlanChargeId             = row[invoiceItemsKeys.index("RatePlanCharge.Id")]
              ratePlanChargeNewQuantity    = 0
              ratePlanChargeQuantity       = row[invoiceItemsKeys.index("RatePlanCharge.Quantity")]
              ratePlanChargeTracking__c    = row[invoiceItemsKeys.index("RatePlanCharge.Tracking__c")]
              ratePlanChargeAvailable__c   = row[invoiceItemsKeys.index("RatePlanCharge.Available__c")]
              ratePlanChargeSmoothingModel = row[invoiceItemsKeys.index("ProductRatePlanCharge.SmoothingModel")]
              ratePlanChargeSubId          = row[invoiceItemsKeys.index("Subscription.Id")]

              ratePlanChargeHash[ratePlanChargeId] = {:ChargeId => ratePlanChargeId, :SubscriptionId => ratePlanChargeSubId, :Quantity => ratePlanChargeQuantity, :Tracking__c => ratePlanChargeTracking__c, :Available__c => ratePlanChargeAvailable__c, :New_Available__c => ratePlanChargeQuantity.to_f, :SmoothingModel => ratePlanChargeSmoothingModel }

              if ratePlanChargeSmoothingModel == "Rollover"
                output_xml, input_xml = soap_call() do |xml, args|
                  xml['ns1'].query do |xml|
                    xml['ns1'].queryString "Select RolloverBalance from RatePlanCharge where Id = ='" + ratePlanChargeId + "'"
                  end 
                end
                rollOverBalance = output_xml.xpath('//ns2:RolloverBalance', 'ns2' =>'http://object.api.zuora.com/').text
                ratePlanChargeHash[ratePlanChargeId][:New_Available__c] = (ratePlanChargeQuantity.to_f + rollOverBalance.to_f).to_s
                ratePlanChargeHash[ratePlanChargeId][:RollOverBalance] = rollOverBalance
              end
            end
          end
        end
      end
    end

    puts '===> Update charges and custom custom fields'
    time = Time.now.to_i.to_s
    APP_ROOT = File.dirname(__FILE__)
    file_name = 'Callout-RatePlanCharge-Update-' + time + '.csv'
    output_file = File.join(APP_ROOT, file_name ) 

    success_charges =[]
    failure_charges = []
    #Create results CSV while batching through and updating charges
    CSV.open(output_file, "w") do |csv|
      #Add Header
      csv << ['Id', 'Status', 'Message', 'Quantity','SmoothingModel', 'Tracking__c (Old)', 'Tracking__c (New)', 'Available__c (old)', 'Available__c (new)', 'SubscriptionId']
      #Batch into 50 objects
      ratePlanChargeHash.each_slice(50) do |batch|
        output_xml, input_xml = soap_call(batch: batch) do |xml, args|
          xml['ns1'].update do
            args[:batch].each do |id, charge|
              xml['ns1'].zObjects('xsi:type' => "ns2:RatePlanCharge") do
                xml['ns2'].Id           id
                xml['ns2'].Tracking__c  "0.00" 
                xml['ns2'].Available__c charge[:New_Available__c]
              end
            end
          end
        end
        
        #Iterate through results batch and wrtie each line to results file 
        output_xml.xpath('//ns1:result', 'ns1' =>'http://api.zuora.com/').each_with_index do |call, i|
          status  = call.xpath('./ns1:Success', 'ns1' =>'http://api.zuora.com/').text
          id      =  call.xpath('./ns1:Id', 'ns1' =>'http://api.zuora.com/').text
          message = status == 'false' ? "#{call.xpath('./*/ns1:Code', 'ns1' =>'http://api.zuora.com/').text}: #{call.xpath('./*/ns1:Message', 'ns1' =>'http://api.zuora.com/').text}" : nil 
          charge  = ratePlanChargeHash[id]

          #If success, push to list for update
          if status == 'true' 
            success_charges.push(id) 
            new_tracking = "0.00"
          else
            failure_charges.push(id)
            new_tracking = charge[:Tracking__c] 
          end

          csv << [id, status, message, charge[:Quantity], charge[:SmoothingModel], charge[:Tracking__c], new_tracking, charge[:Available__c], charge[:New_Available__c], charge[:SubscriptionId]] 
        end  
      end
    end

    if failure_charges.size >0
      Pony.mail( :to => @username, :from => 'Movable_Ink_Invoice_Update_Post', :subject => 'An error occured with operation ', :body => "Job at time #{time}")
    end

    puts '=> Finished '
    File.delete('InvoiceItem.csv')

  rescue => ex
    @error = ex.message
  end
  erb :index    
end 

get '/*' do
  redirect to('/')
end