""" 
    Module for controlling deliveries os samples and projects to castor (THE castor!!!!)
"""
import paramiko
import getpass
import glob
import time
import stat

from deliver import *



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
            sftp_client = MySFTPClient.from_transport(transport)
            # move to the delivery directory in the sftp
            #open one client session and leave it open for all the time of the transfer
            sftp_client.chdir(self.expand_path(self.castordeliverypath))
            #create the project folder in the remote server
            sftp_client.mkdir(self.projectid, ignore_existing=True)
            #move inside the project folder
            sftp_client.chdir(self.projectid)
            #now cycle across the samples
            for sampleid in samples_to_deliver:
                sampleDelivererObj = CastorSampleDeliverer(self.projectid, sampleid, sftp_client)
                st = sampleDelivererObj.deliver_sample()
                status = (status and st)
            # query the database whether all samples in the project have been sucessfully delivered
            if self.all_samples_delivered():
                # this is the only delivery status we want to set on the project level, in order to avoid concurrently
                # running deliveries messing with each other's status updates
                self.update_delivery_status(status="DELIVERED")
                self.acknowledge_delivery()
            try:
                #now I can close the client
                sftp_client.close()
                #and the transport
                transport.close()
            except Exception as e:
                logger.error("Caught exception: {}: {}".format(e.__class__, e))
                raise
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
            origin_folder_sample = os.path.join(self.expand_path(self.stagingpath), self.sampleid)
            #create the sample folder
            self.sftp_client.mkdir(self.sampleid, ignore_existing=True)
            #now target dir is created
            targed_dir = self.sampleid
            self.sftp_client.put_dir(origin_folder_sample ,targed_dir)
            #now copy the md5
            source_md5 = os.path.join(self.expand_path(self.stagingpath), "{}.md5".format(self.sampleid))
            target_md5 = os.path.join(self.sftp_client.getcwd(), "{}.md5".format(self.sampleid))
            self.sftp_client.put(source_md5, target_md5)
        except Exception as e:
            print 'Caught exception: {}: {}'.format(e.__class__, e)
            raise
        logger.info("{} sample transferred to castor sftp server".format(self.sampleid))
        # return True, if something went wrong an exception is thrown before this
        return True

###http://stackoverflow.com/questions/4409502/directory-transfers-on-paramiko
class MySFTPClient(paramiko.SFTPClient):
    
    def rm_dir(self, target):
        ''' Removes recoursevely contents of the target directory.
        Hidden method to be called only for testing purpose (we are not supposed to delver stuff we have delivered
        '''
        for item in self.listdir(target):
            st_mode = self.stat(os.path.join(target, item)).st_mode
            if not stat.S_ISDIR(st_mode):
                self.remove(os.path.join(target, item))
            else:
                self.rm_dir(os.path.join(target, item))
        self.rmdir(target)
    

    def put_dir(self, source, target):
        ''' Uploads the contents of the source directory to the target path. The
            target directory needs to exists. All subdirectories in source are 
            created under target.
        '''
        for item in os.listdir(source):
            
            if os.path.isfile(os.path.join(source, item)):
                self.put(os.path.join(source, item), "{}/{}".format(target, item))
            else:
                self.mkdir("{}/{}".format(target, item), ignore_existing=True)
                self.put_dir(os.path.join(source, item),"{}/{}".format(target, item))

    def mkdir(self, path, mode=511, ignore_existing=False):
        ''' Augments mkdir by adding an option to not fail if the folder exists  '''
        try:
            super(MySFTPClient, self).mkdir(path, mode)
        except IOError:
            if ignore_existing:
                pass
            else:
                raise





