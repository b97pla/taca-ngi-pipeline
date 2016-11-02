""" 
    Module for controlling deliveries os samples and projects to castor (THE castor!!!!)
"""

import paramiko
import getpass
import glob
import time


from deliver import *


## cehcks if a file/folder exists in the remote location
## http://stackoverflow.com/questions/850749/check-whether-a-path-exists-on-a-remote-host-using-paramiko
def rexists(sftp, path):
    """os.path.exists for paramiko's SCP object
    """
    try:
        sftp.stat(path)
    except IOError, e:
        if 'No such file' in str(e):
            return False
        raise
    else:
        return True


class CastorProjectDeliverer(ProjectDeliverer):
    """ This object takes care of delivering project samples to castor's wharf.
    """
    def __init__(self, projectid=None, sampleid=None, **kwargs):
        super(CastorProjectDeliverer, self).__init__(
            projectid,
            sampleid,
            **kwargs)
        # castor specific fields
        self.castordeliverypath = getattr(self, 'castordeliverypath', None)
        self.castorsftpserver = getattr(self, 'castorsftpserver', None)
        self.castorsftpserver_user = getattr(self, 'castorsftpserver_user', None)
    

    def deliver_project(self):
        """ Deliver all samples in a project to castor
            
            :returns: True if all samples were delivered successfully, False if
                any sample was not properly delivered or ready to be delivered
        """
        try:
            logger.info("Delivering {} to {}".format(
                str(self), self.expand_path(self.castordeliverypath)))
            if self.get_delivery_status() == 'DELIVERED' \
                    and not self.force:
                logger.info("{} has already been delivered".format(str(self)))
                return True
            # right now, don't catch any errors since we're assuming any thrown
            # errors needs to be handled by manual intervention
            status = True
            # Open sftp session with castor, in this way multiple tranfer will be possible
            try:
                transport=paramiko.Transport(self.castorsftpserver)
                password = getpass.getpass(prompt='Bianca/Castor Password for user {}:'.format(self.castorsftpserver_user))
                transport.connect(username = "{}-{}".format(self.castorsftpserver_user, self.uppnexid), password = password)
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
            sftp_client.chdir(self.expand_path(self.castordeliverypath))
            #create the project folder in the remote server
            if not rexists(sftp_client, self.projectid):
                sftp_client.mkdir(self.projectid)
            #move inside the project folder
            sftp_client.chdir(self.projectid)
            #now cycle across the samples
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
        A class for handling sample deliveries to castor
    """

    def __init__(self, projectid=None, sampleid=None, sftp_client=None, **kwargs):
        super(CastorSampleDeliverer, self).__init__(
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
            #http://stackoverflow.com/questions/4409502/directory-transfers-on-paramiko
            #walk through the staging path and recreate the same path in castor and put files there
            folder_sample = os.path.join(self.expand_path(self.stagingpath), self.sampleid)
            #create the sample folder
            if not rexists(self.sftp_client, self.sampleid):
                self.sftp_client.mkdir(self.sampleid)
            #move inside the folder
            self.sftp_client.chdir(self.sampleid)
            remote_folder_sample = self.sftp_client.getcwd()
            remote_folder = remote_folder_sample
            for dirpath, dirnames, filenames in os.walk(folder_sample):
                import pdb
                pdb.set_trace()
                #here us where I ned to find a solution!!!!!
                prefix_remote_path = dirpath.split(folder_sample)[1]
                # make remote directory
                for dir in dirnames:
                    if not rexists(self.sftp_client, os.path.join(prefix_remote_path, dir)):
                        self.sftp_client.mkdir( os.path.join(prefix_remote_path, dir) )
                for filename in filenames:
                    local_path = os.path.join(dirpath, filename)
                    self.sftp_client.put(local_path, prefix_remote_path)
            #we can go back to sample level
            self.sftp_client.chdir("../")
        except Exception as e:
            print 'Caught exception: {}: {}'.format(e.__class__, e)
            raise
        logger.info("{} sample transferred to castor sftp server".format(self.sampleid))
        # return True, if something went wrong an exception is thrown before this
        return True




