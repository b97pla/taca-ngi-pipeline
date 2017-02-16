""" 
    Module for controlling deliveries os samples and projects to GRUS
"""
import paramiko
import getpass
import glob
import time
import stat
import requests
import datetime
from dateutil.relativedelta import relativedelta

from ngi_pipeline.database.classes import CharonSession, CharonError
from taca.utils import transfer

from deliver import ProjectDeliverer, SampleDeliverer, DelivererInterruptedError


class GrusProjectDeliverer(ProjectDeliverer):
    """ This object takes care of delivering project samples to castor's wharf.
    """
    def __init__(self, projectid=None, sampleid=None, **kwargs):
        super(GrusProjectDeliverer, self).__init__(
            projectid,
            sampleid,
            **kwargs
        )

    def deliver_project(self):
        """ Deliver all samples in a project to grus
            :returns: True if all samples were delivered successfully, False if
                any sample was not properly delivered or ready to be delivered
        """
        logger.info("Delivering {} to {}".format(
            str(self), self.expand_path(self.castordeliverypath)))
        if self.get_delivery_status() == 'DELIVERED' \
                and not self.force:
            logger.info("{} has already been delivered".format(str(self)))
            return True
        status = True
        try:
            # connect to charon, return list of sample objects
            samples_to_deliver = self.get_staged_samples_from_charon()
        except Exception, e:
            logger.error("Cannot get samples from Charon. Error says: {}".format(str(e)))
            raise

        hard_staged_samples = []
        for sample_id in samples_to_deliver:
            try:
                GrusSampleDeliverer(self.projectid, sample_id).deliver_sample()
            except Exception, e:
                logger.error('Sample {} has not been hard staged. Error says: {}'.format(sample_id, error))
                status = False
            else:
                hard_staged_samples.append(sample)

        if len(samples_to_deliver) != len(hard_staged_samples):
            # do we terminate or do we try to deliver partly?
            logger.warning('Not all the samples have been hard staged. Terminating')
            status = False
            raise something


        try:
            pi_email = self._get_pi_email()
        except Exception, e:
            logger.error("Cannot fetch pi_email from StatusDB. Error says: {}".format(str(e)))
            status = False
            return status

        try:
            pi_id = self._get_pi_id(pi_email)
        except Exception, e:
            logger.error("Cannot fetch pi_id from snic API. Error says: {}".format(str(e)))
            status = False
            return status

        delivery_project_id = self._create_delivery_project(pi_id)
        # if do_delivery failed, no token
        delivery_token = self.do_delivery(delivery_project_id) # instead of to_outbox
        if delivery_token:
            # todo: save delivery_token in Charon
            pass
        else:
            status = False

        return status

    def do_delivery(self):
        pass

    def get_staged_samples_from_charon(self):
        charon_session = CharonSession()
        result = charon_session.project_get_samples(self.projectid)
        samples = result.get('samples')
        if samples is None:
            raise AssertionError('CharonSession returned no results for project {}'.format(self.projectid))

        staged_samples = []
        for sample in samples:
            sample_id = sample.get('sampleid')
            delivery_status = sample.get('delivery_status')
            if delivery_status == 'STAGED':
                staged_samples.append(sample_id)
        return staged_samples


    def _create_delivery_project(self, pi_id):
        # "https://disposer.c3se.chalmers.se/supr-test/api/ngi_delivery/project/create/"
        create_project_url = self.config.get('create_project_url')
        supr_date_format = '%Y-%m-%d'
        today = datetime.date.today()
        six_months_from_now = (today + relativedelta(months=+6))
        data = {
            'ngi_project_name': project,
            'title': "DELIVERY_{}_{}".format(self.projectid, today.strftime(supr_date_format)),
            'pi_id': pi_id,
            'start_date': today,
            'end_date': six_months_from_now.strftime(supr_date_format),
            'continuation_name': '',
            # You can use this field to allocate the size of the delivery
            # 'allocated': size_of_delivery,
            # This field can be used to add any data you like
            'api_opaque_data': '',
            'ngi_ready': False,
            'ngi_delivery_status': ''
        }

        response = requests.post(create_project_url,
                                     data=json.dumps(data),
                                     auth=(user, password))
        # todo: get delivery id
        return response


    def _get_pi_id(self, pi_email):

        get_user_url = self.config.get('get_snic_user_api_url')
        username = self.config.get('snic_username')
        password = self.config.get('snic_password')
        response = requests.get(get_user_url, params={'email_i': pi_email}, auth=(username, password))
        if response.status_code != 200:
            raise AssertionError("Status code returned when trying to get PI id for email: {} was not 200. Response was: {}".format(pi_email, response.content))
        result = json.loads(response.content)
        matches = result["matches"]
        if len(matches) < 1:
            raise AssertionError("There were no hits in SUPR for email: {}".format(pi_email))
        if len(matches) > 1:
            raise AssertionError("There we more than one hit in SUPR for email: {}".format(pi_email))
        pi_id = matches[0].get("id")
        return pi_id


    def _get_pi_email(self):
       # http://genomics_status:et8urph8eg@tools-dev.scilifelab.se:5984
        url = self.config.get('status_db_url')
        status_db_url = 'http://{}:{}@{}:{}'.format(self.config.get('username'), self.config.get('password'), url, self.config.get('port'))
        status_db = self.connect_to_StatusDB(status_db_url)
        orderportal_db = status_db['orderportal_ngi']
        view = orderportal_db.view('test/pi_email_and_project_id')
        rows = view.rows[self.projectid]
        if len(rows) < 1:
            raise AssertionError("Project {} not found in StatusDB: {}".format(self.projecid, url))
        if len(rows) > 1:
            raise AssertionError('Project {} has more than one entry in orderportal_db'.format(self.projectid))

        pi_email = rows[0].value
        return pi_email



class GrusSampleDeliverer(SampleDeliverer):
    """
        A class for handling sample deliveries to castor
    """

    def __init__(self, projectid=None, sampleid=None, **kwargs):
        super(GrusSampleDeliverer, self).__init__(
            projectid,
            sampleid,
            **kwargs)
        self.stagingpathhard = getattr(self, 'stagingpathhard', None)
        if self.stagingpathhard is None:
            raise AttributeError("staginpathhard is required when delivering to GRUS")
        #check if the project directory already exists, if so abort
        hard_stagepath = self.expand_path(self.stagingpathhard)
        if os.path.exists(hard_stagepath):
            logger.error("In {} found already folder {}. No multiple mover deliveries are allowed".format(
                    hard_stagepath, projectid))
            raise DelivererInterruptedError("Hard Staged Folder already present")
        else:
            #otherwise lock the delivery by creating the folder
            create_folder(hard_stagepath)

    def deliver_sample(self, sampleentry=None):
        """ Deliver a sample to the destination specified via command line of on Charon.
            Will check if the sample has already been delivered and should not
            be delivered again or if the sample is not yet ready to be delivered.
            Delivers only samples that have been staged.

            :params sampleentry: a database sample entry to use for delivery,
                be very careful with caching the database entries though since
                concurrent processes can update the database at any time
            :returns: True if sample was successfully delivered or was previously
                delivered, False if sample was not yet ready to be delivered
            :raises taca_ngi_pipeline.utils.database.DatabaseError: if an entry corresponding to this
                sample could not be found in the database
            :raises DelivererReplaceError: if a previous delivery of this sample
                has taken place but should be replaced
            :raises DelivererError: if the delivery failed
        """
        # propagate raised errors upwards, they should trigger notification to operator
        try:
            logger.info("Delivering {} to GRUS with MOVER!!!!!".format(str(self)))
            hard_stagepath = self.expand_path(self.stagingpathhard)
            soft_stagepath = self.expand_path(self.stagingpath)
            try:
                if self.get_delivery_status(sampleentry) != 'STAGED':
                    logger.info("{} has not been stages and will not be delivered".format(str(self)))
                    return False
                if self.get_delivery_status(sampleentry) == 'DELIVERED' \
                        and not self.force:
                    logger.info("{} has already been delivered".format(str(self)))
                    return True
                elif self.get_delivery_status(sampleentry) == 'IN_PROGRESS' \
                        and not self.force:
                    logger.info("delivery of {} is already in progress".format(
                            str(self)))
                    return False
                elif self.get_sample_status(sampleentry) == 'FRESH' \
                        and not self.force:
                    logger.info("{} is marked as FRESH (new unporcessed data is available)and will not be delivered".format(str(self)))
                    return False
                elif not os.path.exists(os.path.join(soft_stagepath,self.sampleid)):
                    logger.info("Sample {} marked as STAGED on charon but not found in the soft stage dir {}".format(str(self), soft_stagepath))
                    return False
                elif self.get_delivery_status(sampleentry) == 'FAILED':
                        logger.info("retrying delivery of previously failed sample {}".format(str(self)))
            except db.DatabaseError as e:
                logger.error("error '{}' occurred during delivery of {}".format(
                        str(e), str(self)))
                raise
            #at this point copywith deferance the softlink folder
            self.update_delivery_status(status="IN_PROGRESS")

            self.do_delivery()
        #in case of faiulure put again the status to STAGED
        except DelivererInterruptedError:
            self.update_delivery_status(status="STAGED")
            raise
        except Exception:
            self.update_delivery_status(status="STAGED")
            raise


    def do_delivery(self):
        """ Creating a hard copy of staged data
            :returns: True if delivery was successful, False if unsuccessful
            :raises DelivererTOBEDEFINEDError: if an exception occurred when creating a hard copy
        """

        logger.info("Creating hard copy of sample {}".format(self.sampleid))

        try:
            if os.path.exists(self.stagingpathhard):

                source_md5 = os.path.join(self.expand_path(self.stagingpath), "{}.md5".format(self.sampleid))
                target_md5 = os.path.join(self.sftp_client.getcwd(), "{}.md5".format(self.sampleid))
                self.sftp_client.put(source_md5, target_md5)
        except Exception as e:
            print 'Caught exception: {}: {}'.format(e.__class__, e)
            raise
        logger.info("Sample {} has been hard staged in {}".format(self.stagingpathhard))
        # return True, if something went wrong an exception is thrown before this
        return True