import reporting.generate_report as gp


class reporting_module:
    def __init__(self, perams):
        self.report_builder = gp.reporting(perams, auto_grouping=None, Logging="DEBUG")
        print("Reporting module is running")
        self.__run__()
        print("Reporting module is done")

    def __run__(self):
        self.report_builder.configure_reporting()
        #when testing just put the service you want to run in the array
        self.report_builder.run_reporting()
        self.report_builder.process_data("BW")
        self.report_builder.process_data("SF")

        #Todo: Add a way to pass in the data to the S3 bucket

# TODO: Add the ability to pass in at runtime the io_number and outher args
creds = None
perams = {
    "io_number": "PF1113BEN10632",
    "celtra_date": "2024-11-01",
    'Bucket': 'adverity',
    'Bucket_Path': 'Bucket_Path',
    "g_folder_id": None,
    "master_spredsheet_id": None,
}


reporting_module(perams)


# report_builder = reporting({'io_number': 'CR725PRO10029', 'g_folder_id': '1BhD_nbkNjz42WmlaAcDh4ljdgwkMt329', "master_spredsheet_id" : None}, auto_grouping=None, Logging='DEBUG')

# report_builder.configure_reporting()

# report_builder.run_reporting()

# report_builder.process_data("BW")
# report_builder.process_data("BW")
# report_builder.build_final_report()
