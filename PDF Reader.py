import os
import time
from csv import writer, QUOTE_MINIMAL
from PyPDF2 import PdfFileReader, PdfFileWriter
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential

filename = "[file name here no file suffix needed]"
blob_connection_string = "[blocb conection string here]"
blob_pdf_container = "formstoscan"
cognitive_endpoint = "https://alroseformreader.cognitiveservices.azure.com/"
ai_services_key = "[Azure services key here]"
form_reader_cognitive_model_id = "027"
#make this better
local_file_path = "c:\\Users\\cahal\\PycharmProjects\\PDFspliter\\" + filename

filename_pdf = filename + ".pdf"
pageNumber = 1

st = time.time()

print("preparing to scan file " + filename_pdf)
pdf_file = open(filename_pdf, 'rb')
pdf_reader = PdfFileReader(pdf_file)
print("Opening Document {}".format(filename_pdf))

pageNumbers = pdf_reader.getNumPages()
number_of_forms = pageNumbers - 1
print("Document has {} AR19 forms".format(pageNumbers - 1))

os.mkdir(filename)
os.chdir(filename)
print("created dump folder {}".format(filename))
with open(filename + '.csv', 'w+', newline='', encoding="utf-8") as csvfile:
    filewriter = writer(csvfile, delimiter=',',
                        quotechar='|', quoting=QUOTE_MINIMAL)
    filewriter.writerow(
        ['batch number', 'date', 'customer', 'order number', 'model', 'quantity', 'stroke', 'fill pressure', 'f1', 'f2',
         'centre to centre', 'inside thread', 'tube diameter', 'rod diameter', 'thread length rod',
         'thread length body', 'end fitting body', 'end fitting rod', 'file name'])
    csvfile.close()
print("created CSV {}".format(filename + ".csv"))

print("--------Splitting started---------")
for i in range(1, pageNumbers):
    pdf_writer = PdfFileWriter()
    pdf_writer.addPage(pdf_reader.getPage(i))
    split_motive = open(filename + '-' + str(pageNumber) + '.pdf', 'wb')
    pageNumber = pageNumber + 1
    pdf_writer.write(split_motive)
    split_motive.close()

print("--------Splitting complete--------")
pdf_file.close()

print("scanning content of file: " + local_file_path)


class AzureBlobFileUploader:
    def __init__(self):
        print("Initializing AzureBlobFileUploader")

        # Initialize the connection to Azure storage account
        self.blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)

    def process_all_files_in_folder(self):
        # Get all files with pdf extension and exclude directories
        all_file_names = [f for f in os.listdir(local_file_path)
                          if os.path.isfile(os.path.join(local_file_path, f)) and ".pdf" in f]

        # Upload each file
        for file_name in all_file_names:
            self.process_file(file_name)

    def process_file(self, file_name):
        # Create blob with same name as local file name
        blob_client = self.blob_service_client.get_blob_client(container=blob_pdf_container,
                                                               blob=file_name)
        # Get full path to the file
        upload_file_path = os.path.join(local_file_path, file_name)

        # Create blob on storage
        # Overwrite if it already exists!
        file_content_setting = ContentSettings(content_type='pdf')
        print(f"uploading file to blob - {file_name}")
        with open(upload_file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True, content_settings=file_content_setting)
            form_url = "https://ar19training.blob.core.windows.net/formstoscan/" + file_name
            print("preparing to analyze document at " + form_url)
            document_analysis_client = DocumentAnalysisClient(
                endpoint=cognitive_endpoint, credential=AzureKeyCredential(ai_services_key)
            )

            poller = document_analysis_client.begin_analyze_document_from_url(form_reader_cognitive_model_id, form_url)
            result = poller.result()

            for idx, document in enumerate(result.documents):
                print("Analyzing document " + file_name)
                print("Document has type {}".format(document.doc_type))
                print("Document has confidence {}".format(document.confidence))
                print("Document was analyzed by model with ID {}".format(result.model_id))
                values = ['batch number', 'date', 'customer', 'order number', 'model', 'quantity', 'stroke',
                          'fill pressure', 'f1', 'f2', 'centre to centre', 'inside thread', 'tube diameter',
                          'rod diameter', 'thread length rod', 'thread length body', 'end fitting body',
                          'end fitting rod', file_name]
                for name, field in document.fields.items():
                    field_value = field.value if field.value else field.content
                    print("......found field '{}' of type '{}' with value '{}' and with confidence {}".format(name,
                                                                                                              field.value_type,
                                                                                                              field_value,
                                                                                                              field.confidence))
                    values[:] = [field_value if x == name else x for x in values]
            print("writing data to CSV")
            with open(filename + '.csv', 'a', newline='', encoding="utf-8") as csvfile_amend:
                writer_object = writer(csvfile_amend)
                writer_object.writerow(values)
                csvfile_amend.close()
            print(f"deleting file from blob - {file_name}")
            blob_client.delete_blob()


# Initialize class and upload files
azure_blob_file_uploader = AzureBlobFileUploader()
azure_blob_file_uploader.process_all_files_in_folder()

et = time.time()
elapsed_time = et - st
print('Execution time:', elapsed_time, 'seconds')
