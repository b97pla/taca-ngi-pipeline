""" CLI for the deliver subcommand
"""
import click
import logging
import taca.utils.misc
from deliver import deliver as _deliver

logger = logging.getLogger(__name__)

#######################################
## deliver                           
#######################################

@click.group()
@click.pass_context
@click.option('--deliverypath', type=click.STRING,
			  help="Deliver to this destination folder")
@click.option('--stagingpath', type=click.STRING,
			  help="Stage the delivery under this path")
@click.option('--uppnexid', type=click.STRING,
			  help="Use this UppnexID instead of fetching from database")
@click.option('--operator', type=click.STRING, default=None, multiple=True,
			  help="Email address to notify operator at. Multiple operators can be specified")
@click.option('--stage_only', is_flag=True, default=False,
			  help="Only stage the delivery but do not transfer any files")
@click.option('--force', is_flag=True, default=False,
			  help="Force delivery, even if e.g. analysis has not finished or "\
                  "sample has already been delivered")
def deliver(ctx,deliverypath,stagingpath,uppnexid,operator,stage_only,force):
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
        
## project delivery
@deliver.command()
@click.pass_context
@click.argument('projectid',type=click.STRING,nargs=-1)
def project(ctx, projectid):
    """ Deliver the specified projects to the specified destination
    """
    for pid in projectid:
        d = _deliver.ProjectDeliverer(
            pid,
            **ctx.parent.params)
        _exec_fn(d,d.deliver_project)
    
## sample delivery
@deliver.command()
@click.pass_context
@click.argument('projectid',type=click.STRING,nargs=1)
@click.argument('sampleid',type=click.STRING,nargs=-1)
def sample(ctx, projectid, sampleid):
    """ Deliver the specified sample to the specified destination
    """
    for sid in sampleid:
        d = _deliver.SampleDeliverer(
            projectid,
            sid,
            **ctx.parent.params)
        _exec_fn(d,d.deliver_sample)


# helper function to handle error reporting
def _exec_fn(obj,fn):
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
                content="Project: {}\nSample: {}\nCommand: {}\n\n"\
                    "Additional information:{}\n".format(
                        obj.projectid,
                        obj.sampleid,
                        str(fn),
                        str(e)
                    ),
                receiver=obj.config.get('operator'))
        except Exception as me:
            logger.error(
                "processing {} failed - reason: {}, but operator {} could not "\
                "be notified - reason: {}".format(
                    str(obj),e,obj.config.get('operator'),me))
        else:
            logger.error("processing {} failed - reason: {}, operator {} has been "\
                "notified".format(
                    str(obj),str(e),obj.config.get('operator')))
