""" 
    Module for controlling deliveries os samples and projects to Mosler (THE MOSLER!!!!)
"""



from deliver import *


class ProjectDeliverer(ProjectDeliverer):
    """ This object takes care of delivering project samples to mosler.
        When delivering to Mosler the delivery can take place only at project level
    """
    def __init__(self, projectid=None, sampleid=None, **kwargs):
        super(ProjectDeliverer, self).__init__(
            projectid,
            sampleid,
            **kwargs)

    def deliver_project(self):
        """ Deliver all samples in a project to mosler
            
            :returns: True if all samples were delivered successfully, False if
                any sample was not properly delivered or ready to be delivered
        """
        import pdb
        pdb.set_trace()
        try:
            logger.info("Delivering {} to {}".format(
                str(self), self.expand_path(self.deliverypath)))
            if self.get_delivery_status() == 'DELIVERED' \
                    and not self.force:
                logger.info("{} has already been delivered".format(str(self)))
                return True
            # right now, don't catch any errors since we're assuming any thrown 
            # errors needs to be handled by manual intervention
            status = True
            for sampleid in [sentry['sampleid'] for sentry in db.project_sample_entries(
                    db.dbcon(), self.projectid).get('samples', [])]:
                st = SampleDeliverer(self.projectid, sampleid).deliver_sample()
                status = (status and st)
            # query the database whether all samples in the project have been sucessfully delivered
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

    def expand_path(self, path):
        """ Will set the delivery path on mosler sftp server
            which is /UPPMAXID
            
            :params string path: the path to expand
            :returns: the supplied path will all placeholders substituted with
                the corresponding instance attributes
            :raises DelivererError: if a corresponding attribute for a 
                placeholder could not be found
        """
        path = "/{}".format(self.uppnexid)
        return path



class SampleDeliverer(SampleDeliverer):



    def do_delivery(self):
        """ Deliver the staged delivery folder using sftp
            :returns: True if delivery was successful, False if unsuccessful
            :raises DelivererTOBEDEFINEDError: if an exception occurred during
                transfer
        """
        #tar sample directory
        #transfer it (maybe an open session is needed)
        import pdb
        pdb.set_trace()
        #delete the tar







