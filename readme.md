Progress Report Splitter
========================


AWS Lambda function to send email to recipients with attached report files


Deployment
----------
Reference <br>
<a href="https://github.com/aws/chalice">https://github.com/aws/chalice</a>


Sample Request
--------------
	- API endpoint: https://4ea3q4oe9h.execute-api.us-east-1.amazonaws.com/api/new
	- Request method: POST
	- Request header: 
			{
				'x-api-key': '7D6E97ED-E667-4C1F-9075-020CCC5C97EF'
			}
	- Request payload:
		* For CSV file
	    
		    {
		        "report_name": "ali_test",
		        "org_id": "test_org",
		        "course_name": "test_course_name",
		        "course_id": "test_course_id",
		        "filter_message": "test_filter_message",
		        "generated_timestamp": "2021-1-22",
		        "email_addresses": ["XXXXX@xx.com"],
		        "email_sender": "XXXXX@xxxx.com",
		        "group_by": ["ref_State", "ref_City"],
			"CSV_filepath": "sample_03.csv",
		        "email_subject": "6 Apr testing"
	    	     }

	        * for ZIP file

	    	     {
		        "report_name": "ali_test",
		        "org_id": "test_org",
			"courses": 
			{
				"csv01_name": {
					"course_name": "test_course_name01",
		        		"course_id": "test_course_id01",
				},
				"csv02_name": {
					"course_name": "test_course_name02",
		        		"course_id": "test_course_id02",
				}
			},
		        
		        "filter_message": "test_filter_message",
		        "generated_timestamp": "2021-1-22",
		        "email_addresses": ["XXXXX@xx.com"],
		        "email_sender": "XXXXX@xxxx.com",
		        "group_by": ["ref_State", "ref_City"],
		        "ZIP_filepath": "sample_03.zip",
		        "email_subject": "6 Apr testing"
	    	     }

