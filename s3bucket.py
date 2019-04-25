from collections import namedtuple
from functools import reduce
from itertools import chain

from dateutil import parser, relativedelta
from funcy import compact

# Storage:
s3_standard_first_50TB = 0.023
s3_standard_next_450TB = 0.022
s3_standard_over_500TB = 0.021
glacier_storage_rate = 0.004

# PUT requests
s3_standard_1000_PUT_requests = 0.005

# Glacier transition requests
glacier_transition_1000_requests = 0.05

LifecycleRule = namedtuple('LifecycleRule', ['transition', 'period'])

class S3UploadEntry():

    def __init__(self, avg_obj_size, objects_num=1):
        
        self._number_of_objects = objects_num
        self._object_size = avg_obj_size
        self._storage_class = 's3standard'

    def get_total_size(self):
        """Calulate total size of upload entry in GBs
        
        Returns:
            float -- total size of upload entry in GBs
        """
        return self._number_of_objects * self._object_size

    def get_num_of_objects(self):
        """Returns number of objects in upload entry
        
        Returns:
            int -- number of objects
        """
        return self._number_of_objects

    def get_storage_class(self):
        """Returns storage class of the upload entry
        
        Returns:
            string -- upload entry storage class name
        """
        return self._storage_class

    def move_to_glacier(self):
        """Update the upload entry storage class value to 'glacier'"""
        self._storage_class = 'glacier'


class S3Bucket():

    def __init__(self):
        self._data = {}
        self._lifecycle_rules = []

    def add_lifecycle_rule(self, rule):
        """Add lifecycle rule to the bucket
        
        Arguments:
            rule {LifecycleRule} -- lifecycle rule namedtuple to add to the bucket
        """
        self._lifecycle_rules.append(rule)

    
    def apply_lifecycle_rules(self, current_month):
        """Check and apply lifecycle rules if there are any for specified month
           Apply means updating storage_class value for upload entries in the bucket
        
        Arguments:
            current_month {[type]} -- [description]
        """
        for rule in self._lifecycle_rules:
            oldest_data_to_keep = S3Bucket._substr_months(current_month, rule.period)
            keys_to_migrate = filter(lambda el: el < oldest_data_to_keep, self.s3standard_objects().keys())
            for k in keys_to_migrate:
                self.apply_transition(k, rule.transition)


    def apply_transition(self, month, transition):
        """Update storage class value for all the upload entries for the specified month
           Storage class value is set to transition argument value
        
        Arguments:
            month {string} -- month identified by year and month number. String in format YYYY-mm
            transition {string} -- name of the storage class to set
        """
        upload_entries = filter(
            lambda el: el.get_storage_class != transition,
            self._data[month]
        )

        if transition == 'glacier':
            for ue in upload_entries:
                ue.move_to_glacier()
        elif transition == 'somethingelse':
            #TO DO 
            pass

    def s3standard_objects(self):
        """Returns all the data in the bucket with storage_class=s3standard
        
        Returns:
            dict -- dict where keys are months and values are corresponding lists of upload entries
        """
        return compact({
            k:list(filter(
                lambda el: el.get_storage_class()=='s3standard', v)
            ) for k,v in self._data.items()})

    def upload_data(self, month, data):
        """Upload data to S3 bucket for specified period
        
        Arguments:
            month {string} -- month identified by year and month number. String in format YYYY-mm
            data {S3UploadEntry} -- S3UploadEntry object to upload to the bucket
        """
        if month in self._data:
            self._data[month].append(data)
        else:
            self._data[month] = [data]


    def generate_report(self):
        """Create a report with all charges for each month

        Returns:
            dict -- dict with monthly charges where keys are billing month and values are dicts of monthly reports
        """
        result = {}
        for m in self._data.keys():
            self.apply_lifecycle_rules(m)
            result[m] = self.get_monthly_charges(m)

        return result

    def get_totals(self):
        """Calculates total charges for all the months by charge types (storage, requests, etc)
        
        Returns:
            dict -- dict where keys are charge types and values are amount of $ to pay
        """

        result = {}
        report = self.generate_report()

        total_storage = reduce(
            lambda x,y: x + y['s3_standard_storage_charges']+y['s3_glacier_storage_charges'],
            report.values(),
            0
        )
        total_put_requests = reduce(
            lambda x,y: x + y['put_request_charges'],
            report.values(),
            0
        )
        total_transitions = reduce(
            lambda x,y: x + y['transition_charges'],
            report.values(),
            0
        )

        return {
            'storage_charges': total_storage,
            'put_request_charges': total_put_requests,
            'transition_charges': total_transitions,
        }

    def get_monthly_charges(self, month):
        """Calculates monthly charges for the specified month
        
        Arguments:
            month {string} -- month identified by year and month number. String in format YYYY-mm
        
        Returns:
            dict -- dict where keys are charge types (storage, PUT requests, transition) and values are charge amounts in $
        """
        s3_standard_volume = self.get_month_volume(month)
        glacier_volume = self.get_month_volume(month, 'glacier')
        put_requests = self.get_month_requests(month)
        transitions = self.get_month_transitions(month)

        return {
            's3_standard_storage_charges': S3Bucket._get_s3_standard_storage_charges(s3_standard_volume),
            's3_glacier_storage_charges': S3Bucket._get_glacier_storage_charges(glacier_volume),
            'put_request_charges': S3Bucket._get_request_charges(put_requests),
            'transition_charges': S3Bucket._get_transition_charges(transitions),
            's3st_volume': self.get_month_volume(month),
            'glacier_volume': self.get_month_volume(month,'glacier'),
        }


    def get_month_volume(self, month, storage_class='s3standard'):
        """Calculates total volume of data which has been written to the bucket till specified month
           By default, volume is calculated for s3standard storage class data. Other possible values: glacier 
        
        Arguments:
            month {string} -- month identified by year and month number. String in format YYYY-mm
        
        Returns:
            float -- volume of data written in GB
        """
        months_till_now = filter(lambda k: k <= month, self._data.keys())
        upload_entries = chain(*[self._data[k] for k in months_till_now])
        upload_entries = filter(
                lambda el: el.get_storage_class() == storage_class,
                upload_entries
        )
        return sum(map(lambda el: el.get_total_size(), upload_entries))

    def get_month_requests(self, month):
        """Calculates number of PUT requests to the bucket for the specified month
        
        Arguments:
            month {string} -- month identified by year and month number. String in format YYYY-mm
        
        Returns:
            int -- number of PUT requests
        """
        if month in self._data:
            upload_entries = self._data[month]
            return sum(map(lambda el: el.get_num_of_objects(), upload_entries))
        return 0

    def get_month_transitions(self, month):
        """Checks if there are any transitions to apply in specified month
        
        Arguments:
            month {string} -- month identified by year and month number. String in format YYYY-mm
        
        Returns:
            dict -- dict where keys are transition names (glacier, expire) and values are numbers of GBs to migrate
        """
        
        # Check if there is any transition to apply this month
        transitions = filter(
                lambda r: S3Bucket._substr_months(month, r.period) in self._data, 
                self._lifecycle_rules
        )

        result = {}
        for t in transitions:
            month_to_migrate = S3Bucket._substr_months(month, t.period)
            result[t.transition] = sum(map(lambda el: el.get_num_of_objects(), self._data[month_to_migrate]))
        
        return result

    def _substr_months(m1, m2):
        """Substracts m2 months from m1 month
        
        Arguments:
            m1 {string} -- month identified by year and month number. String in format YYYY-mm
            m2 {int} -- num of months to substract
        
        Returns:
            string -- result month identified by year and month number. String in format YYYY-mm
        """
        m1 = parser.parse(m1)
        m2 = relativedelta.relativedelta(months=m2)
        r = m1 - m2
        return r.strftime('%Y-%m')        
        
    def _get_s3_standard_storage_charges(volume):
        """Calculate storage charges for a given volume for s3 standard storage class
        
        Arguments:
            volume {float} -- volume of data stored in the bucket
        
        Returns:
            float -- amount of $ to pay for storage
        """
        if volume >= 500000:
            return 50000 * s3_standard_first_50TB + \
                   450000 * s3_standard_next_450TB + \
                   (volume - 500000) * s3_standard_over_500TB
        elif volume >= 50000:
            return 50000 * s3_standard_first_50TB + \
                   (volume - 50000) * s3_standard_next_450TB
        elif volume >= 0:
            return volume * s3_standard_first_50TB

    def _get_glacier_storage_charges(volume):
        """Calculate storage charges for a given volume for glacier storage class
        
        Arguments:
            volume {float} -- volume of data stored in the bucket
        
        Returns:
            float -- amount of $ to pay for storage
        """
        return volume * glacier_storage_rate

    def _get_request_charges(num):
        """Calculates charges for PUT requests
        
        Arguments:
            num {int} -- number of PUT requests
        
        Returns:
            float -- amount of $ to pay for PUT requests
        """
        return num / 1000 * s3_standard_1000_PUT_requests

    def _get_transition_charges(transitions):
        """Calculates charges for transition requests. Currently only glacier transition is supported
        
        Arguments:
            transitions {string} -- name of transition. Currently only glacier transition is supported
        
        Returns:
            float -- amount of  $ to pay for the transition
        """
        try:
            return transitions['glacier'] / 1000 * glacier_transition_1000_requests 
        except KeyError:
            return 0
