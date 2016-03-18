""" 
    Module for controlling deliveries os samples and projects to Mosler (THE MOSLER!!!!)
"""

import paramiko
import getpass
import tarfile
import threading
import glob
import time


from deliver import *




class MoslerProjectDeliverer(ProjectDeliverer):
    """ This object takes care of delivering project samples to mosler.
        When delivering to Mosler the delivery can take place only at project level
    """
    def __init__(self, projectid=None, sampleid=None, **kwargs):
        super(MoslerProjectDeliverer, self).__init__(
            projectid,
            sampleid,
            **kwargs)
    

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
                transport=paramiko.Transport(self.moslersftpserver)
                password = getpass.getpass(prompt='Mosler Password for user {}:'.format(self.moslersftpserver_user))
                transport.connect(username = self.moslersftpserver_user, password = password)
            except Exception as e:
                print 'Caught exception: {}: {}'.format(e.__class__, e)
                raise

            #memorize all samples that needs to be delivered
            samples_to_deliver = [sentry['sampleid'] for sentry in db.project_sample_entries(
                    db.dbcon(), self.projectid).get('samples', [])]
            
            # run multiple threads and store return functions
            # http://stackoverflow.com/questions/6893968/how-to-get-the-return-value-from-a-thread-in-python
            threads = [None] * len(samples_to_deliver)
            results = [None] * len(samples_to_deliver)
            thread  = 0
            while len(samples_to_deliver) > 0:
                # check how many samples there are in mosler sftp server
                # create an sftp client for each sample (only one put can be done in one client. This was needed for the threaded version)
                sftp_client = transport.open_sftp_client()
                # move to the delivery directory in the sftp
                sftp_client.chdir(self.expand_path(self.moslerdeliverypath))
                # Count number of files
                num_tar_files_mosler = len(sftp_client.listdir('.'))
                # if the the number of remote tar files is higher the the maximum number of deliveries wait
                if num_tar_files_mosler >= self.moslersftpmaxfiles:
                    logger.info("More than {} files in Mosler sftp server".format(self.moslersftpmaxfiles))
                    print "more than {} files in Mosler".format(self.moslersftpmaxfiles)
                    sftp_client.close()
                    # wait 10 minutes
                    time.sleep(600)
                    continue
                # otherwise take next sample
                sampleid = samples_to_deliver.pop()
                sampleDelivererObj = MoslerSampleDeliverer(self.projectid, sampleid, sftp_client)
                st = sampleDelivererObj.deliver_sample()
                # initiate the thread and give it the return index
                #threads[thread] = threading.Thread(target=sampleDelivererObj.deliver_sample_thread, args=(None, results, thread))
                #threads[thread].start()
                #take a nap not need to rush
                status = (status and st) # I need to wait for the last job and then cehck the status....
            try:
                transport.close()
            except Exception as e:
                print 'Caught exception: {}: {}'.format(e.__class__, e)
                raise
            # query the database whether all samples in the project have been sucessfully delivered
            if self.all_samples_delivered():
                # this is the only delivery status we want to set on the project level, in order to avoid concurrently
                # running deliveries messing with each other's status updates
                self.update_delivery_status(status="DELIVERED")
                #self.acknowledge_delivery()
            return status
        except (db.DatabaseError, DelivererInterruptedError, Exception):
            raise





class MoslerSampleDeliverer(SampleDeliverer):
    """
        A class for handling sample deliveries to Mosler
    """

    def __init__(self, projectid=None, sampleid=None, sftp_client=None, **kwargs):
        super(MoslerSampleDeliverer, self).__init__(
            projectid,
            sampleid,
            **kwargs)
        self.sftp_client = sftp_client


    def deliver_sample_thread(self, sampleentry=None, result=None, index=None):
        result[index] = self.deliver_sample(sampleentry)
        return None


    def do_delivery(self):
        """ Deliver the staged delivery folder using sftp
            :returns: True if delivery was successful, False if unsuccessful
            :raises DelivererTOBEDEFINEDError: if an exception occurred during
                transfer
        """
        # tar sample directory before moving it to mosler
        # stolen from http://stackoverflow.com/questions/2032403/how-to-create-full-compressed-tar-file-using-python
        # get the folder where tar file will be stored (same as the project delivery)
        sample_tar_location = self.expand_path(self.stagingpath)
        # create the tar archve
        sample_tar_archive = os.path.join(sample_tar_location, "{}.tar".format(self.sampleid))
        sample_tar = tarfile.open(sample_tar_archive, "w", dereference=True)
        # add to the archive the directory
        logger.info("{} building tar file for sample".format(self.sampleid))
        sample_tar.add(os.path.join(sample_tar_location,  "{}".format(self.sampleid)), arcname="{}".format(self.sampleid))
        # close the tar ball
        logger.info("{} tar file for sample builded".format(self.sampleid))
        sample_tar.close()
        # transfer it (maybe an open session is needed)
        logger.info("{} transferring sample to nestor sftp server".format(self.sampleid))
        try:
            self.sftp_client.put(sample_tar_archive, "{}.tar".format(self.sampleid))
        except Exception as e:
            print 'Caught exception: {}: {}'.format(e.__class__, e)
            raise
        logger.info("{} sample transferred to nestor sftp server".format(self.sampleid))
        # delete the tar
        os.remove(os.path.join(sample_tar_location,  "{}.tar".format(self.sampleid)))
        # close the sftp client, I will not reuse it anymore
        self.sftp_client.close()
        # return True, if something went wrong an exception is thrown before this
        return True


