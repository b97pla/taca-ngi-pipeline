"""
    Module for controlling deliveries of samples and projects
"""
import datetime
import glob
import json
import logging
import os
import re
import signal

from ngi_pipeline.database import classes as db
from ngi_pipeline.utils.classes import memoized
from taca.utils.config import CONFIG
from taca.utils.filesystem import create_folder, chdir
from taca.utils.misc import hashfile, call_external_command
from taca.utils import transfer

logger = logging.getLogger(__name__)

class DelivererError(Exception): pass
class DelivererDatabaseError(DelivererError): pass
class DelivererInterruptedError(DelivererError): pass
class DelivererReplaceError(DelivererError): pass
class DelivererRsyncError(DelivererError): pass

def _signal_handler(signal, frame):
    """ A custom signal handler which will raise a DelivererInterruptedError
        :raises DelivererInterruptedError: 
            this exception will be raised
    """
    raise DelivererInterruptedError(
        "interrupt signal {} received while delivering".format(signal))

def _timestamp(days=None):
    """Current date and time (UTC) in ISO format, with millisecond precision.
    Add the specified offset in days, if given.
    Stolen from https://github.com/NationalGenomicsInfrastructure/charon/blob/master/charon/utils.py
    """
    instant = datetime.datetime.utcnow()
    if days:
        instant += datetime.timedelta(days=days)
    instant = instant.isoformat()
    return instant[:-9] + "%06.3f" % float(instant[-9:]) + "Z"

class Deliverer(object):
    """ 
        A (abstract) superclass with functionality for handling deliveries
    """
    def __init__(self, projectid, sampleid, **kwargs):
        """
            :param string projectid: id of project to deliver
            :param string sampleid: id of sample to deliver
            :param bool no_checksum: if True, skip the checksum computation
            :param string hash_algorithm: algorithm to use for calculating 
                file checksums, defaults to sha1
        """
        # override configuration options with options given on the command line
        self.config = CONFIG.get('deliver',{})
        self.config.update(kwargs)
        # set items in the configuration as attributes
        for k, v in self.config.items():
            setattr(self,k,v)
        self.projectid = projectid
        self.sampleid = sampleid
        self.hash_algorithm = getattr(
            self,'hash_algorithm','sha1')
        self.no_checksum = getattr(
            self,'no_checksum',False)
        self.ngi_node = getattr(
            self,'ngi_node','unknown')
        # only set an attribute for uppnexid if it's actually given or in the db
        try:
            t = self.uppnexid
        except AttributeError:
            try:
                t = self.project_entry()['uppnex_id']
                self.uppnexid = t
            except KeyError:
                pass
        # set a custom signal handler to intercept interruptions
        signal.signal(signal.SIGINT,_signal_handler)
        signal.signal(signal.SIGTERM,_signal_handler)

    def __str__(self):
        return "{}:{}".format(
            self.projectid,self.sampleid) \
            if self.sampleid is not None else self.projectid

    def acknowledge_delivery(self, tstamp=_timestamp()):
        try:
            ackfile = self.expand_path(
                os.path.join(self.deliverystatuspath,"{}_delivered.ack".format(
                    self.sampleid or self.projectid)))
            create_folder(os.path.dirname(ackfile))
            with open(ackfile,'w') as fh:
                fh.write("{}\n".format(tstamp))
        except (AttributeError, IOError) as e:
            logger.warning(
                "could not write delivery acknowledgement, reason: {}".format(
                    e))

    @memoized
    def dbcon(self):
        """ Establish a CharonSession
            :returns: a ngi_pipeline.database.classes.CharonSession instance
        """
        return db.CharonSession()

    def db_entry(self):
        """ Abstract method, should be implemented by subclasses """
        raise NotImplementedError("This method should be implemented by "\
        "subclass")

    def project_entry(self):
        """ Fetch a database entry representing the instance's project
            :returns: a json-formatted database entry
            :raises DelivererDatabaseError:
                if an error occurred when communicating with the database
        """
        return self.wrap_database_query(
            self.dbcon().project_get,self.projectid)

    def project_sample_entries(self):
        """ Fetch the database sample entries representing the instance's project
            :returns: a list of json-formatted database sample entries
            :raises DelivererDatabaseError: 
                if an error occurred when communicating with the database
        """
        return self.wrap_database_query(
            self.dbcon().project_get_samples,self.projectid)

    def update_delivery_status(self, *args, **kwargs):
        """ Abstract method, should be implemented by subclasses """
        raise NotImplementedError("This method should be implemented by "\
        "subclass")
    
    def wrap_database_query(self,query_fn,*query_args,**query_kwargs):
        """ Wrapper calling the supplied method with the supplied arguments
            :param query_fn: function reference in the CharonSession class that
                will be called
            :returns: the result of the function call
            :raises DelivererDatabaseError: 
                if an error occurred when communicating with the database
        """
        try:
            return query_fn(*query_args,**query_kwargs)
        except db.CharonError as ce:
            raise DelivererDatabaseError(ce.message)

    def get_analysis_status(self, dbentry=None):
        """ Returns the analysis status for this sample. If a sampleentry
            dict is supplied, it will be used instead of fethcing from database

            :params sampleentry: a database sample entry to use instead of
                fetching from db
            :returns: the analysis status of this sample as a string
        """
        dbentry = dbentry or self.db_entry()
        return dbentry.get('analysis_status','TO_ANALYZE')

    def get_delivery_status(self, dbentry=None):
        """ Returns the delivery status for this sample. If a sampleentry
            dict is supplied, it will be used instead of fethcing from database

            :params sampleentry: a database sample entry to use instead of
                fetching from db
            :returns: the delivery status of this sample as a string
        """
        dbentry = dbentry or self.db_entry()
        return dbentry.get('delivery_status','NOT_DELIVERED')

    def gather_files(self, patterns=[]):
        """ This method will locate files matching the patterns specified in 
            the config and compute the checksum and construct the staging path
            according to the config.
            
            The config should contain the key 'files_to_deliver', which should
            be a list of tuples with source path patterns and destination path
            patterns. The source path can be a file glob and can refer to a 
            folder or file. File globs will be expanded and folders will be
            traversed to include everything beneath.
             
            :returns: A generator of tuples with source path, 
                destination path and the checksum of the source file 
                (or None if source is a folder)
        """
        def _get_digest(sourcepath,destpath):
            digest = None
            if not self.no_checksum:
                checksumpath = "{}.{}".format(sourcepath,self.hash_algorithm)
                try:
                    with open(checksumpath,'r') as fh:
                        digest = fh.next()
                except IOError as re:
                    digest = hashfile(sourcepath,hasher=self.hash_algorithm)
                    try:
                        with open(checksumpath,'w') as fh:
                            fh.write(digest)
                    except IOError as we:
                        logger.warning(
                            "could not write checksum {} to file {}:" \
                            " {}".format(digest,checksumpath,we))
            return (sourcepath,destpath,digest)
            
        def _walk_files(currpath, destpath):
            # if current path is a folder, return all files below it
            if (os.path.isdir(currpath)):
                parent = os.path.dirname(currpath)
                for parentdir,_,dirfiles in os.walk(currpath,followlinks=True):
                    for currfile in dirfiles:
                        fullpath = os.path.join(parentdir,currfile)
                        # the relative path will be used in the destination path
                        relpath = os.path.relpath(fullpath,parent)
                        yield (fullpath,os.path.join(destpath,relpath))
            else:
                yield (currpath,
                    os.path.join(
                        destpath,
                        os.path.basename(currpath)))

        for sfile, dfile in patterns:
            dest_path = self.expand_path(dfile)
            src_path = self.expand_path(sfile)
            matches = 0
            for f in glob.iglob(src_path):
                for spath, dpath in _walk_files(f,dest_path):
                    # ignore checksum files
                    if not spath.endswith(".{}".format(self.hash_algorithm)):
                        matches += 1
                        # skip and warn if a path does not exist, this includes broken symlinks
                        if os.path.exists(spath):
                            yield _get_digest(spath,dpath)
                        else:
                            logger.warning("path {} does not exist, possibly " \
                                "because of a broken symlink".format(spath))
            if matches == 0:
                logger.warning("no files matching search expression '{}' "\
                    "found ".format(src_path))

    def stage_delivery(self):
        """ Stage a delivery by symlinking source paths to destination paths 
            according to the returned tuples from the gather_files function. 
            Checksums will be written to a digest file in the staging path. 
            Failure to stage individual files will be logged as warnings but will
            not terminate the staging. 
            
            :raises DelivererError: if an unexpected error occurred
        """
        digestpath = self.staging_digestfile()
        filelistpath = self.staging_filelist()
        create_folder(os.path.dirname(digestpath))
        try: 
            with open(digestpath,'w') as dh, open(filelistpath,'w') as fh:
                agent = transfer.SymlinkAgent(None, None, relative=True)
                for src, dst, digest in self.gather_files(
                    patterns=getattr(self,'files_to_deliver',[])):
                    agent.src_path = src
                    agent.dest_path = dst
                    try:
                        agent.transfer()
                    except (transfer.TransferError, transfer.SymlinkError) as e:
                        logger.warning("failed to stage file '{}' when "\
                            "delivering {} - reason: {}".format(
                                src,str(self),e))

                    fpath = os.path.relpath(
                        dst,
                        self.expand_path(self.stagingpath))
                    fh.write("{}\n".format(fpath))
                    if digest is not None:
                        dh.write("{}  {}\n".format(digest,fpath))
                # finally, include the digestfile in the list of files to deliver
                fh.write("{}\n".format(os.path.basename(digestpath)))
        except IOError as e:
            raise DelivererError(
                "failed to stage delivery - reason: {}".format(e))
        return True

    def delivered_digestfile(self):
        """
            :returns: path to the file with checksums after delivery
        """
        return self.expand_path(
            os.path.join(
                self.deliverypath,
                os.path.basename(self.staging_digestfile())))

    def staging_digestfile(self):
        """
            :returns: path to the file with checksums after staging
        """
        return self.expand_path(
            os.path.join(
                self.stagingpath,
                "{}.{}".format(self.sampleid,self.hash_algorithm)))

    def staging_filelist(self):
        """
            :returns: path to the file with a list of files to transfer
                after staging
        """
        return self.expand_path(
            os.path.join(
                self.stagingpath,
                "{}.lst".format(self.sampleid)))

    def transfer_log(self):
        """
            :returns: path prefix to the transfer log files. The suffixes will
                be created by the transfer command
        """
        return self.expand_path(
            os.path.join(
                self.logpath,
                "{}_{}".format(self.sampleid,
                    datetime.datetime.now().strftime("%Y%m%dT%H%M%S"))))
                
    @memoized
    def expand_path(self,path):
        """ Will expand a path by replacing placeholders with correspondingly 
            named attributes belonging to this Deliverer instance. Placeholders
            are specified according to the pattern '_[A-Z]_' and the 
            corresponding attribute that will replace the placeholder should be
            identically named but with all lowercase letters.
            
            For example, "this/is/a/path/to/_PROJECTID_/and/_SAMPLEID_" will
            expand by substituting _PROJECTID_ with self.projectid and 
            _SAMPLEID_ with self.sampleid
            
            If the supplied path does not contain any placeholders or is None,
            it will be returned unchanged.
            
            :params string path: the path to expand
            :returns: the supplied path will all placeholders substituted with
                the corresponding instance attributes
            :raises DelivererError: if a corresponding attribute for a 
                placeholder could not be found
        """
        try:
            m = re.search(r'(_[A-Z]+_)',path)
        except TypeError:
            return path
        else:
            if m is None:
                return path
            try:
                expr = m.group(0)
                return self.expand_path(
                    path.replace(expr,getattr(self,expr[1:-1].lower())))
            except AttributeError as e:
                raise DelivererError(
                    "the path '{}' could not be expanded - reason: {}".format(
                        path,e))
    
class ProjectDeliverer(Deliverer):
    
    def __init__(self, projectid=None, sampleid=None, **kwargs):
        super(ProjectDeliverer,self).__init__(
            projectid,
            sampleid,
            **kwargs)
    
    def all_samples_delivered(
        self,
        sampleentries=None):
        """ Checks the delivery status of all project samples

            :params sampleentries: a list of sample entry dicts to use instead
                of fetching from database
            :returns: True if all samples in this project has been successfully
                delivered, False otherwise
        """
        sampleentries = sampleentries or \
            self.project_sample_entries().get('samples',[])
        return all([
            self.get_delivery_status(sentry) == 'DELIVERED' \
            for sentry in sampleentries])

    def create_report(self):
        """ Create a final aggregate report via a system call """
        logprefix = os.path.abspath(
            self.expand_path(os.path.join(self.logpath,self.projectid)))
        try:
            if not create_folder(os.path.dirname(logprefix)):
                logprefix = None
        except AttributeError as e:
             logprefix = None
        with chdir(self.expand_path(self.reportpath)):
            cl = [
                "ngi_reports",
                "ign_aggregate_report",
                "-n",
                self.ngi_node
            ]
            call_external_command(
                cl,
                with_log_files=(logprefix is not None),
                prefix="{}_aggregate".format(logprefix))

    def db_entry(self):
        """ Fetch a database entry representing the instance's project
            :returns: a json-formatted database entry
            :raises DelivererDatabaseError:
                if an error occurred when communicating with the database
        """
        return self.project_entry()

    def deliver_project(self):
        """ Deliver all samples in a project to the destination specified by 
            deliverypath
            
            :returns: True if all samples were delivered successfully, False if
                any sample was not properly delivered or ready to be delivered
        """
        try:
            logger.info("Delivering {} to {}".format(
                str(self),self.expand_path(self.deliverypath)))
            if self.get_delivery_status() == 'DELIVERED' \
                and not self.force:
                logger.info("{} has already been delivered".format(str(self)))
                return True
            # right now, don't catch any errors since we're assuming any thrown 
            # errors needs to be handled by manual intervention
            status = True
            for sampleid in [sentry['sampleid'] \
                for sentry in self.project_sample_entries().get('samples',[])]:
                st = SampleDeliverer(self.projectid,sampleid).deliver_sample()
                status = (status and st)
            # query the database whether all samples in the project have been sucessfully delivered
            if self.all_samples_delivered():
                # this is the only delivery status we want to set on the project level, in order to avoid concurrently running deliveries messing with each other's status updates
                self.update_delivery_status(status="DELIVERED")
                self.acknowledge_delivery()
                logger.info("creating final aggregated report")
                self.create_report()
            return status
        except (DelivererDatabaseError, DelivererInterruptedError, Exception) as e:
            raise

    def update_delivery_status(self, status="DELIVERED"):
        """ Update the delivery_status field in the database to the supplied 
            status for the project specified by this instance
            :returns: the result from the underlying api call
            :raises DelivererDatabaseError: 
                if an error occurred when communicating with the database
        """
        return self.wrap_database_query(
            self.dbcon().project_update,
            self.projectid,
            delivery_status=status)
            
class SampleDeliverer(Deliverer):
    """
        A class for handling sample deliveries
    """
    def __init__(self, projectid=None, sampleid=None, **kwargs):
        super(SampleDeliverer,self).__init__(
            projectid,
            sampleid,
            **kwargs)

    def create_report(self):
        """ Create a sample report and an aggregate report via a system call """
        logprefix = os.path.abspath(
            self.expand_path(os.path.join(self.logpath,"{}-{}".format(
                self.projectid,self.sampleid))))
        try:
            if not create_folder(os.path.dirname(logprefix)):
                logprefix = None
        except AttributeError as e:
             logprefix = None
        with chdir(self.expand_path(self.reportpath)):
            # create the ign_sample_report for this sample
            cl = [
                "ngi_reports",
                "ign_sample_report",
                "-n",
                self.ngi_node,
                "--samples",
                self.sampleid
            ]
            call_external_command(
                cl,
                with_log_files=(logprefix is not None),
                prefix="{}_sample".format(logprefix))
            # estimate the delivery date for this sample to 0.5 days ahead
            cl = [
                "ngi_reports",
                "ign_aggregate_report",
                "-n",
                self.ngi_node,
                "--samples_extra",
                json.dumps({self.sampleid: {"delivered": _timestamp(days=0.5)}})
            ]
            call_external_command(
                cl,
                with_log_files=(logprefix is not None),
                prefix="{}_aggregate".format(logprefix))

    def db_entry(self):
        """ Fetch a database entry representing the instance's project and sample
            :returns: a json-formatted database entry
            :raises DelivererDatabaseError:
                if an error occurred when communicating with the database
        """
        return self.wrap_database_query(
            self.dbcon().sample_get,self.projectid,self.sampleid)

    def deliver_sample(self, sampleentry=None):
        """ Deliver a sample to the destination specified by the config.
            Will check if the sample has already been delivered and should not
            be delivered again or if the sample is not yet ready to be delivered.

            :params sampleentry: a database sample entry to use for delivery,
                be very careful with caching the database entries though since
                concurrent processes can update the database at any time
            :returns: True if sample was successfully delivered or was previously
                delivered, False if sample was not yet ready to be delivered
            :raises DelivererDatabaseError: if an entry corresponding to this
                sample could not be found in the database
            :raises DelivererReplaceError: if a previous delivery of this sample
                has taken place but should be replaced
            :raises DelivererError: if the delivery failed
        """
        # propagate raised errors upwards, they should trigger notification to operator
        try:
            logger.info("Delivering {} to {}".format(
                str(self),self.expand_path(self.deliverypath)))
            try:
                if self.get_analysis_status(sampleentry) != 'ANALYZED' \
                    and not self.force:
                    logger.info("{} has not finished analysis and will not be "\
                        "delivered".format(str(self)))
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
                    logger.info("retrying delivery of previously failed "\
                    "sample {}".format(str(self)))
            except DelivererDatabaseError as e:
                logger.error(
                    "error '{}' occurred during delivery of {}".format(
                        str(e),str(self)))
                raise
            # set the delivery status to in_progress which will also mean that any concurrent deliveries will leave this sample alone
            self.update_delivery_status(status="IN_PROGRESS")
            # an error with the reports should not abort the delivery, so handle
            try:
                if self.report is True:
                    logger.info("creating sample report")
                    self.create_report()
            except AttributeError as e:
                pass
            except Exception as e:
                logger.warning(
                    "failed to create reports for {}, reason: {}".format(
                        self,e))
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
        except DelivererInterruptedError as e:
            self.update_delivery_status(status="NOT DELIVERED")
            raise
        except Exception as e:
            self.update_delivery_status(status="FAILED")
            raise

    def do_delivery(self):
        """ Deliver the staged delivery folder using rsync
            :returns: True if delivery was successful, False if unsuccessful
            :raises DelivererRsyncError: if an exception occurred during
                transfer
        """
        agent = transfer.RsyncAgent(
            self.expand_path(self.stagingpath),
            dest_path=self.expand_path(self.deliverypath),
            digestfile=self.delivered_digestfile(),
            remote_host=getattr(self,'remote_host', None), 
            remote_user=getattr(self,'remote_user', None), 
            log=logger,
            opts={
                '--files-from': [self.staging_filelist()],
                '--copy-links': None,
                '--recursive': None,
                '--perms': None,
                '--chmod': 'u+rwX,og-rwx',
                '--verbose': None,
                '--exclude': ["*rsync.out","*rsync.err"]
            })
        create_folder(os.path.dirname(self.transfer_log()))
        try:
            return agent.transfer(transfer_log=self.transfer_log())
        except transfer.TransferError as e:
            raise DelivererRsyncError(e)

    def update_delivery_status(self, status="DELIVERED"):
        """ Update the delivery_status field in the database to the supplied 
            status for the project and sample specified by this instance
            :returns: the result from the underlying api call
            :raises DelivererDatabaseError: 
                if an error occurred when communicating with the database
        """
        return self.wrap_database_query(
            self.dbcon().sample_update,
            self.projectid,
            self.sampleid,
            delivery_status=status)
            
            
