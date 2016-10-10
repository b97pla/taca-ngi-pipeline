""" 
    Module for controlling deliveries os samples and projects to Mosler (THE MOSLER!!!!)
"""

import paramiko
import getpass
import threading
import glob
import time


from deliver import *




class CastorProjectDeliverer(ProjectDeliverer):
    """ This object takes care of delivering project samples to castor's wharf.
    """
    def __init__(self, projectid=None, sampleid=None, **kwargs):
        super(CastorProjectDeliverer, self).__init__(
            projectid,
            sampleid,
            **kwargs)
        # Mosler specific fields
        self.moslerdeliverypath = getattr(self, 'castordeliverypath', None)
        self.moslersftpserver = getattr(self, 'castorsftpserver', None)
        self.moslersftpserver_user = getattr(self, 'castorsftpserver_user', None)
    

    def deliver_project(self):
        """ Deliver all samples in a project to mosler
            
            :returns: True if all samples were delivered successfully, False if
                any sample was not properly delivered or ready to be delivered
        """
        try:
            logger.info("Delivering {} to {}".format(
                str(self), self.expand_path(self.moslerdeliverypath)))
            if self.get_delivery_status() == 'DELIVERED' \
                    and not self.force:
                logger.info("{} has already been delivered".format(str(self)))
                return True
            # right now, don't catch any errors since we're assuming any thrown 
            # errors needs to be handled by manual intervention
            status = True
            # Open sftp session with mosler, in this way multiple tranfer will be possible
            try:
                transport=paramiko.Transport(self.castorsftpserver)
                password = getpass.getpass(prompt='Bianca/Castor Password for user {}:'.format(self.moslersftpserver_user))
                transport.connect(username = self.moslersftpserver_user, password = password)
            except Exception as e:
                logger.error("Caught exception: {}: {}".format(e.__class__, e))
                raise

            #memorize all samples that needs to be delivered
            samples_to_deliver = [sentry['sampleid'] for sentry in db.project_sample_entries(
                    db.dbcon(), self.projectid).get('samples', [])]
            status = True
            #open the sftp client
            sftp_client = transport.open_sftp_client()
            # move to the delivery directory in the sftp
            #open one client session and leave it open for all the time of the transfer
            sftp_client.chdir(self.expand_path(self.moslerdeliverypath))
            for sampleid in samples_to_deliver:
                sampleDelivererObj = CastorSampleDeliverer(self.projectid, sampleid, sftp_client)
                st = sampleDelivererObj.deliver_sample()
                status = (status and st)
            try:
                transport.close()
            except Exception as e:
                logger.error("Caught exception: {}: {}".format(e.__class__, e))
                raise
            # close the client
            self.sftp_client.close()
            # query the database whether all samples in the project have been sucessfully delivered
            if self.all_samples_delivered():
                # this is the only delivery status we want to set on the project level, in order to avoid concurrently
                # running deliveries messing with each other's status updates
                self.update_delivery_status(status="DELIVERED")
                self.acknowledge_delivery()
            return status
        except (db.DatabaseError, DelivererInterruptedError, Exception):
            raise





class CastorSampleDeliverer(SampleDeliverer):
    """
        A class for handling sample deliveries to Mosler
    """

    def __init__(self, projectid=None, sampleid=None, sftp_client=None, **kwargs):
        super(MoslerSampleDeliverer, self).__init__(
            projectid,
            sampleid,
            **kwargs)
        self.sftp_client = sftp_client


    def do_delivery(self):
        """ Deliver the staged delivery folder using sftp
            :returns: True if delivery was successful, False if unsuccessful
            :raises DelivererTOBEDEFINEDError: if an exception occurred during
                transfer
        """
        # transfer it (maybe an open session is needed)
        logger.info("{} transferring sample to castor sftp server".format(self.sampleid))
        try:
            #http://unix.stackexchange.com/questions/7004/uploading-directories-with-sftp
            #http://stackoverflow.com/questions/4409502/directory-transfers-on-paramiko
            #
            #most likely need first to make the dest folder and chnage other stuff but need to wait for bianca to be in place
            self.sftp_client.put(sample_tar_archive, "{}".format(self.sampleid))
        except Exception as e:
            print 'Caught exception: {}: {}'.format(e.__class__, e)
            raise
        logger.info("{} sample transferred to castor sftp server".format(self.sampleid))
        # return True, if something went wrong an exception is thrown before this
        return True


