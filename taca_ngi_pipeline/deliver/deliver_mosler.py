""" 
    Module for controlling deliveries os samples and projects to Mosler (THE MOSLER!!!!)
"""

import paramiko
import getpass

from deliver import *

class MoslerDeliverer(Deliverer):

    def __init__(self, projectid=None, sampleid=None, **kwargs):
        super(MoslerDeliverer, self).__init__(
            projectid,
            sampleid,
            **kwargs)
        self.moslerdeliverypath = getattr(self, 'moslerdeliverypath', None)
        self.moslersftpserver = getattr(self, 'moslersftpserver', None)
        self.moslersftpserver_user = getattr(self, 'moslersftpserver_user', None)


class MoslerProjectDeliverer(MoslerDeliverer):
    """ This object takes care of delivering project samples to mosler.
        When delivering to Mosler the delivery can take place only at project level
    """
    def __init__(self, projectid=None, sampleid=None, **kwargs):
        super(MoslerProjectDeliverer, self).__init__(
            projectid,
            sampleid,
            **kwargs)
    
    def db_entry(self):
        """ Fetch a database entry representing the instance's project
            :returns: a json-formatted database entry
            :raises taca_ngi_pipeline.utils.database.DatabaseError:
                if an error occurred when communicating with the database
            THIS HAS BEEN COPIED AND PASTED FROM deliver.py ProjectDeliverer class
        """
        return db.project_entry(db.dbcon(), self.projectid)
    
    def update_delivery_status(self, status="DELIVERED"):
        """ Update the delivery_status field in the database to the supplied 
            status for the project specified by this instance
            :returns: the result from the underlying api call
            :raises taca_ngi_pipeline.utils.database.DatabaseError:
                if an error occurred when communicating with the database
            THIS HAS BEEN COPIED AND PASTED FROM deliver.py ProjectDeliverer class
        """
        return db.update_project(db.dbcon(), self.projectid, delivery_status=status)

    def deliver_project(self):
        """ Deliver all samples in a project to mosler
            
            :returns: True if all samples were delivered successfully, False if
                any sample was not properly delivered or ready to be delivered
        """

        ### TODO: I need to open here an sft connection

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
                sftp_client = transport.open_sftp_client()
                # move to the delivery directory in the sftp
                sftp_client.chdir(self.expand_path(self.moslerdeliverypath))
            except Exception as e:
                print 'Caught exception: {}: {}'.format(e.__class__, e)
                raise



            for sampleid in [sentry['sampleid'] for sentry in db.project_sample_entries(
                    db.dbcon(), self.projectid).get('samples', [])]:
                import pdb
                pdb.set_trace()
                # pass to the constructor also the sftp_client object
                st = MoslerSampleDeliverer(self.projectid, sampleid, sftp_client).deliver_sample()
                status = (status and st)
            #now close connection with sftp server
            try:
                sftp_client.close()
                transport.close()
            except Exception as e:
                print 'Caught exception: {}: {}'.format(e.__class__, e)
                raise

            # query the database whether all samples in the project have been sucessfully delivered
            import pdb
            pdb.set_trace()
            if self.all_samples_delivered():
                # this is the only delivery status we want to set on the project level, in order to avoid concurrently
                # running deliveries messing with each other's status updates
                self.update_delivery_status(status="DELIVERED")
                self.acknowledge_delivery()
                # create the final aggregate report
                try:
                    if self.report_aggregate:
                        logger.info("creating final aggregate report")
                        self.create_report()
                except AttributeError as e:
                    pass
                except Exception as e:
                    logger.warning(
                        "failed to create final aggregate report for {}, "\
                        "reason: {}".format(self,e))
            return status
        except (db.DatabaseError, DelivererInterruptedError, Exception):
            raise





class MoslerSampleDeliverer(MoslerDeliverer):
    """
        A class for handling sample deliveries to Mosler
    """

    def __init__(self, projectid=None, sampleid=None, sftp_client=None, **kwargs):
        super(MoslerSampleDeliverer, self).__init__(
            projectid,
            sampleid,
            **kwargs)
        self.sftp_client = sftp_client

    def deliver_sample(self, sampleentry=None):
        """ Deliver a sample to the destination specified by the config.
            Will check if the sample has already been delivered and should not
            be delivered again or if the sample is not yet ready to be delivered.

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
            THIS HAS BEEN COPIED AND PASTED FROM deliver.py SampletDeliverer class
        """
        # propagate raised errors upwards, they should trigger notification to operator
        try:
            logger.info("Delivering {} to {}".format(
                str(self), self.expand_path(self.deliverypath)))
            try:
                if self.get_analysis_status(sampleentry) != 'ANALYZED' \
                        and not self.force:
                    logger.info("{} has not finished analysis and will not be delivered".format(str(self)))
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
                elif self.get_delivery_status(sampleentry) == 'FAILED':
                    logger.info("retrying delivery of previously failed sample {}".format(str(self)))
            except db.DatabaseError as e:
                logger.error(
                    "error '{}' occurred during delivery of {}".format(
                        str(e), str(self)))
                raise
            # set the delivery status to in_progress which will also mean that any concurrent deliveries
            # will leave this sample alone
            self.update_delivery_status(status="IN_PROGRESS")
            # an error with the reports should not abort the delivery, so handle
            #Skipping report generation for MOSLER.... I have more urgen problems right now....
            #try:
            #    if self.report_sample and self.report_aggregate:
            #        logger.info("creating sample reports")
            #        self.create_report()
            #except AttributeError:
            #    pass
            #except Exception as e:
            #    logger.warning(
            #        "failed to create reports for {}, reason: {}".format(
            #            self, e))
            # stage the delivery
            if not self.stage_delivery():
                raise DelivererError("sample was not properly staged")
            logger.info("{} successfully staged".format(str(self)))
            if not self.stage_only:
                # perform the delivery
                if not self.do_delivery():
                    raise DelivererError("sample was not properly delivered")
                logger.info("{} successfully delivered".format(str(self)))
                # set the delivery status in database
                self.update_delivery_status()
                # write a delivery acknowledgement to disk
                self.acknowledge_delivery()
            return True
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
        #tar sample directory
        #stolen from http://stackoverflow.com/questions/2032403/how-to-create-full-compressed-tar-file-using-python
        #import tarfile
        #tar = tarfile.open("sample.tar.gz", "w:gz")
        #for name in ["file1", "file2", "file3"]:
        #    tar.add(name)
        #tar.close()
        
        #transfer it (maybe an open session is needed)
        try:
            self.sftp_client.put('/home/vezzi/software/taca-ngi-pipeline/requirements.txt', 'requirments.txt')
        except Exception as e:
            print 'Caught exception: {}: {}'.format(e.__class__, e)
            raise
        import pdb
        pdb.set_trace()
        #delete the tar

    def db_entry(self):
        """ Fetch a database entry representing the instance's project and sample
            :returns: a json-formatted database entry
            :raises taca_ngi_pipeline.utils.database.DatabaseError:
                if an error occurred when communicating with the database
            THIS HAS BEEN COPIED AND PASTED FROM deliver.py SampletDeliverer class
        """
        return db.sample_entry(db.dbcon(), self.projectid, self.sampleid)

    def update_delivery_status(self, status="DELIVERED"):
        """ Update the delivery_status field in the database to the supplied 
            status for the project and sample specified by this instance
            :returns: the result from the underlying api call
            :raises taca_ngi_pipeline.utils.database.DatabaseError:
                if an error occurred when communicating with the database
            THIS HAS BEEN COPIED AND PASTED FROM deliver.py SampletDeliverer class
        """
        return db.update_sample(db.dbcon(), self.projectid, self.sampleid, delivery_status=status)




