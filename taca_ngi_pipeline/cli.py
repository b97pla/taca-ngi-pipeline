""" CLI for the deliver subcommand
"""
import click
import logging

from ngi_pipeline.database.classes import CharonSession

import taca.utils.misc
from deliver import deliver as _deliver
from deliver import deliver_mosler as _deliver_mosler
from deliver import deliver_castor as _deliver_castor
from deliver import deliver_grus as _deliver_grus

logger = logging.getLogger(__name__)

#######################################
# deliver
#######################################


@click.group()
@click.pass_context
@click.option('--deliverypath', type=click.STRING, help="Deliver to this destination folder")
@click.option('--stagingpath', type=click.STRING, help="Stage the delivery under this path")
@click.option('--uppnexid', type=click.STRING, help="Use this UppnexID instead of fetching from database")
@click.option('--operator', type=click.STRING, default=None, multiple=True,
              help="Email address to notify operator at. Multiple operators can be specified")
@click.option('--stage_only', is_flag=True, default=False,
              help="Only stage the delivery but do not transfer any files")
@click.option('--force', is_flag=True, default=False,
              help="Force delivery, even if e.g. analysis has not finished or sample has already been delivered")
@click.option('--cluster', default="milou",  type=click.Choice(['milou', 'mosler', 'bianca', 'grus']),
              help="Specify to which cluster one wants to deliver")

def deliver(ctx, deliverypath, stagingpath, uppnexid, operator, stage_only, force, cluster):
    """ Deliver methods entry point
    """
    if deliverypath is None:
        del ctx.params['deliverypath']
    if stagingpath is None:
        del ctx.params['stagingpath']
    if uppnexid is None:
        del ctx.params['uppnexid']
    if operator is None or len(operator) == 0:
        del ctx.params['operator']

# deliver subcommands

# project delivery
@deliver.command()
@click.pass_context
@click.argument('projectid', type=click.STRING, nargs=-1)
def project(ctx, projectid):
    """ Deliver the specified projects to the specified destination
    """
    if ctx.parent.params['cluster'] == 'bianca':
        if len(projectid) > 1:
            logger.error("Only one project can be specified when delivering to Bianca. Specficied {} projects".format(len(projectid)))
            return 1
    for pid in projectid:
        if ctx.parent.params['cluster'] == 'milou':
            d = _deliver.ProjectDeliverer(
                pid,
                **ctx.parent.params)
        elif ctx.parent.params['cluster'] == 'mosler':
            d = _deliver_mosler.MoslerProjectDeliverer(
                pid,
                **ctx.parent.params)
        elif ctx.parent.params['cluster'] == 'bianca':
            d = _deliver_castor.CastorProjectDeliverer(
                pid,
                **ctx.parent.params)
        elif ctx.parent.params['cluster'] == 'grus':
            d = _deliver_grus.GrusProjectDeliverer(
                pid,
                **ctx.parent.params)
        _exec_fn(d, d.deliver_project)

# sample delivery
@deliver.command()
@click.pass_context
@click.argument('projectid', type=click.STRING, nargs=1)
@click.argument('sampleid', type=click.STRING, nargs=-1)
def sample(ctx, projectid, sampleid):
    """ Deliver the specified sample to the specified destination
    """
    if ctx.parent.params['cluster'] == 'bianca':
        #in this case I need to open a sftp connection in order to avoid to insert password everytime
        projectObj = _deliver_castor.CastorProjectDeliverer(projectid, **ctx.parent.params)
        projectObj.create_sftp_connnection()
        #create the project folder in the remote server
        #move to delivery folder
        projectObj.sftp_client.chdir(projectObj.config.get('castordeliverypath', '/wharf'))
        projectObj.sftp_client.mkdir(projectid, ignore_existing=True)
        #move inside the project folder
        projectObj.sftp_client.chdir(projectid)
    for sid in sampleid:
        if ctx.parent.params['cluster'] == 'milou':
            d = _deliver.SampleDeliverer(
                projectid,
                sid,
                **ctx.parent.params)
        elif ctx.parent.params['cluster'] == 'mosler':
            d = _deliver_mosler.MoslerSampleDeliverer(
                projectid,
                sid,
                **ctx.parent.params)
        elif ctx.parent.params['cluster'] == 'bianca':
            d = _deliver_castor.CastorSampleDeliverer(
                projectid,
                sid,
                sftp_client=projectObj.sftp_client,
                **ctx.parent.params)
        elif ctx.parent.params['cluster'] == 'grus':
            d = _deliver_grus.GrusSampleDeliverer(
                projectid,
                sid,
                **ctx.parent.params)
        _exec_fn(d, d.deliver_sample)
    if ctx.parent.params['cluster'] == 'bianca':
        projectObj.close_sftp_connnection()


# helper function to handle error reporting
def _exec_fn(obj, fn):
    try:
        if fn():
            logger.info(
                "{} processed successfully".format(str(obj)))
        else:
            logger.info(
                "{} processed with some errors, check log".format(
                    str(obj)))
    except Exception as e:
        try:
            taca.utils.misc.send_mail(
                subject="[ERROR] processing failed: {}".format(str(obj)),
                content="Project: {}\nSample: {}\nCommand: {}\n\nAdditional information:{}\n".format(
                    obj.projectid, obj.sampleid, str(fn), str(e)),
                receiver=obj.config.get('operator'))
        except Exception as me:
            logger.error(
                "processing {} failed - reason: {}, but operator {} could not be notified - reason: {}".format(
                    str(obj), e, obj.config.get('operator'), me))
        else:
            logger.error("processing {} failed - reason: {}, operator {} has been notified".format(
                str(obj), str(e), obj.config.get('operator')))


@deliver.command()
@click.pass_context
@click.argument('projectid', type=click.STRING, nargs=-1)
def check_grus_delivery_status(context, projectid=None):
    if projectid is not None:
        charon_session = CharonSession()
        project = charon_session.project_get(projectid)
        delivery_token = project.get('delivery_token')
        delivery_status = project.get('delivery_status')
        if delivery_status == 'IN_PROGRESS' and delivery_token:
            logger.info('Checking the delivery status')
            # todo: how do we check if it's done or not??
            # this api:  /api/project/search/ ?
