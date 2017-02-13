""" 
    Module for controlling deliveries os samples and projects to castor (THE castor!!!!)
"""
import paramiko
import getpass
import glob
import time
import stat

from deliver import *



class GrusProjectDeliverer(ProjectDeliverer):
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
    
    def create_sftp_connnection(self):
        try:
            self.transport=paramiko.Transport(self.castorsftpserver)
            password = getpass.getpass(prompt='Bianca/Castor Password for user {}:'.format(self.castorsftpserver_user))
            self.transport.connect(username = "{}-{}".format(self.castorsftpserver_user, self.uppnexid), password = password)
        except Exception as e:
            logger.error("Caught exception: {}: {}".format(e.__class__, e))
            raise
        #open the sftp client
        self.sftp_client = MySFTPClient.from_transport(self.transport)

    def close_sftp_connnection(self):
        try:
            #now I can close the client
            self.sftp_client.close()
            #and the transport
            self.transport.close()
        except Exception as e:
            logger.error("Caught exception: {}: {}".format(e.__class__, e))
            raise

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
            status = True
            #open one client session and leave it open for all the time of the transfer
            self.create_sftp_connnection()
            #memorize all samples that needs to be delivered
            samples_to_deliver = [sentry['sampleid'] for sentry in db.project_sample_entries(
                    db.dbcon(), self.projectid).get('samples', [])]
            status = True
            # move to the delivery directory in the sftp
            self.sftp_client.chdir(self.expand_path(self.castordeliverypath))
            #create the project folder in the remote server
            self.sftp_client.mkdir(self.projectid, ignore_existing=True)
            #move inside the project folder
            self.sftp_client.chdir(self.projectid)
            #now cycle across the samples
            for sampleid in samples_to_deliver:
                sampleDelivererObj = CastorSampleDeliverer(self.projectid, sampleid, self.sftp_client)
                st = sampleDelivererObj.deliver_sample()
                status = (status and st)
            # query the database whether all samples in the project have been sucessfully delivered
            if self.all_samples_delivered():
                # this is the only delivery status we want to set on the project level, in order to avoid concurrently
                # running deliveries messing with each other's status updates
                self.update_delivery_status(status="DELIVERED")
                self.acknowledge_delivery()
            #close connection
            self.close_sftp_connnection()
            return status
        except (db.DatabaseError, DelivererInterruptedError, Exception):
            raise





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
                elif not os.path.exists(os.path.join(soft_stagepath, str(self))):
                    logger.info("Sample {} marked as STAGED on charon but not found in the soft stage dir {}".format(                            str(self), soft_stagepath))
                    return False
                elif self.get_delivery_status(sampleentry) == 'FAILED':
                        logger.info("retrying delivery of previously failed sample {}".format(str(self)))
            except db.DatabaseError as e:
                logger.error("error '{}' occurred during delivery of {}".format(
                        str(e), str(self)))
                raise
            #at this point copywith deferance the softlink folder
            self.update_delivery_status(status="IN_PROGRESS")
            #call do_delivery
        except DelivererInterruptedError:
            self.update_delivery_status(status="NOT DELIVERED")
            raise
        except Exception:
            self.update_delivery_status(status="FAILED")
            raise







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




