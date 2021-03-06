library('ggplot2')
library('grid')
library('RSQLite')

args <- commandArgs(TRUE)

if ( length(args) < 1 ) {
    stop('require DB_TOPDIR. Usage: Rscript plotter.R <DB_TOPDIR> [<TIME_PERIOD>]')
}

# load data from SQLite
db_dpath  <- args[1]

## construct a pattern as 'mm_trackTorqueJobs_201402.db' if time_month is given
## otherwise, 'mm_trackTorqueJobs_[[:digit:]]*.db'
pattern  <- 'mm_trackTorqueJobs_[[:digit:]]*.db'
o_suffix <- ''
t_suffix <- ''
if ( length( args ) >= 2 ) {
    pattern  <- paste('mm_trackTorqueJobs_', args[2], '.db', sep='')
    o_suffix <- paste('_',args[2],sep='')
    t_suffix <- paste(': period ',args[2],sep='')
}

db_files  <- list.files(path=db_dpath, pattern=pattern)

if ( length( db_files ) == 0 ) {
    stop('No data files.')
}

if ( length( db_files ) > 1 ) {
    o_suffix <- ''
}

drv <- dbDriver('SQLite')

job_data <- data.frame('queue' = character(0),
                       'cstat' = character(0),
                       'uid'   = character(0),
                       'rwtime'= integer(0),
                       'cwtime'= integer(0),
                       'cctime'= integer(0),
                       'rmem'  = numeric(0),
                       'cmem'  = numeric(0),
                       't_start' = integer(0),
                       't_queue' = integer(0))

for (f in db_files) {
    db_fpath   <- paste(db_dpath,f,sep='/')
    print( paste('Quering database',db_fpath,sep=' '))
    t_job_data <- tryCatch( {
                      conn <- dbConnect(drv, db_fpath)
                      dbGetQuery(conn,'SELECT queue,cstat,uid,rmem,rwtime,cmem,
                                       cwtime,cctime,t_start,t_queue FROM jobs
                                       WHERE queue NOT LIKE \'%test\' AND cwtime IS NOT NULL AND cctime < cwtime')
                  },
                  error = function(e) {
                      print( paste('Fail to query database',db_fpath,sep=' '))
                      return(NA)
                  },
                  finally = { dbDisconnect(conn) } )
 
    if ( ! is.na(t_job_data) ) {
        job_data <- rbind(job_data, t_job_data)
    }
}
dbUnloadDriver(drv)

# get data frame for making summary table
getSubDataframeForSummary <- function(data, queue) {

    ## get subset of data by the given queue name
    if ( queue != 'all' ) {
        sd <- subset(data, data$queue == queue)
    } else {
        sd <- data
    }

    ## get column slice for summary information
    df <- data.frame('queue'            = sd$queue,
                     'status'           = sd$cstat,
                     'waitingTime(sec)' = sd$t_start - sd$t_queue,
                     'memoryReq(GB)'    = sd$rmem,
                     'memoryUsed(GB)'   = sd$cmem,
                     'memoryUtilFract'  = 1.0 * sd$cmem / sd$rmem,
                     'walltimeReq(hr)'  = sd$rwtime / 3600.,
                     'walltimeUsed(hr)' = sd$cwtime / 3600.,
                     'cputimeUsed(hr)'  = sd$cctime / 3600.,
                     'cpuUtilFract'     = 1.0 * sd$cctime / sd$cwtime)
    return(df)
}

## making plots 
## 0. common settings for different plots
plot_odir     <- paste(db_dpath, 'plots', sep='/') 

legend_labels <- scale_color_discrete(name  ="job status",
                                      breaks=c("csuccess", "cfailed", "killed"),
                                      labels=c("success", "failed", "killed"))

legend_style  <- theme(legend.position   = 'right',
                       legend.title      = element_text(size=14, face='bold'),
                       legend.text       = element_text(size=12),
                       legend.key.size   = unit(1,'cm'))

hr_ticks_3d <- c(0,2,4,12,24,48,72)
hr_ticks_5d <- append(hr_ticks_3d, c(96,120))
GB_ticks    <- c(0,4,8,16,32,64,128,256)
frac_ticks  <- seq(0,100,10)

hr_range_3d <- c(0,72)
hr_range_5d <- c(0,120)
GB_range    <- c(0,256)

## writing statistical summary to file ##
f_summary <- file( paste(plot_odir, paste('summary',o_suffix,'.txt', sep=''), sep='/' ) )
sink( file=f_summary, type = c('output') )
cat("==============================\n")
cat(paste('Summary', t_suffix, "\n", sep=' '))
cat("==============================\n\n")
summary( getSubDataframeForSummary( job_data, 'all' ), maxsum=12 )
sink( file=NULL )
close(f_summary)

## job waiting time vs. queue
ggsave(filename = paste(plot_odir, paste('qtime_queue_scatter', o_suffix, '.png',sep=''), sep='/'),
       plot     = ggplot(job_data, aes(y=1.0*(t_start-t_queue)/3600., x=queue)) +
                         geom_jitter(aes(color=cstat), alpha=0.5, size=1) +
                         geom_boxplot(outlier.shape=NA, alpha=0.5) +
                         facet_wrap(~ cstat, ncol=1) +
                         ggtitle(paste('Job Waiting Time', t_suffix ,sep=' ')) +
                         xlab('queue') +
                         ylab('hours') +
                         theme_bw() +
                         theme(legend.position="none") +
                         scale_fill_hue(l=45) +
                         scale_y_sqrt(breaks=hr_ticks_3d, limits=hr_range_3d),
                         #legend_labels + legend_style,
       width    = 27,
       height   = 21,
       units    = 'cm')

## job walltime request vs. queue
ggsave(filename = paste(plot_odir, paste('rwtime_queue_scatter', o_suffix, '.png',sep=''), sep='/'),
       plot     = ggplot(job_data, aes(y=rwtime/3600., x=queue)) +
                         geom_jitter(aes(color=cstat), alpha=0.5, size=1) + 
                         geom_boxplot(outlier.shape=NA, alpha=0.5) +
                         facet_wrap(~ cstat, ncol=1) +
                         ggtitle(paste('Job Walltime Request', t_suffix, sep=' ')) +
                         xlab('queue') +
                         ylab('hours') +
                         theme_bw() +
                         theme(legend.position="none") +
                         scale_fill_hue(l=45) +
                         scale_y_sqrt(breaks=hr_ticks_3d, limits=hr_range_3d),
                         #legend_labels + legend_style,
       width    = 27,
       height   = 21,
       units    = 'cm')

## job walltime consumption vs. queue
ggsave(filename = paste(plot_odir, paste('cwtime_queue_scatter', o_suffix, '.png',sep=''),sep='/'),
       plot     = ggplot(job_data, aes(y=cwtime/3600., x=queue)) +
                         geom_jitter(aes(color=cstat), alpha=0.5, size=1) + 
                         geom_boxplot(outlier.shape=NA, alpha=0.5) +
                         facet_wrap(~ cstat, ncol=1) +
                         ggtitle(paste('Job Walltime Consumption', t_suffix, sep=' ')) +
                         xlab('queue') +
                         ylab('hours') +
                         theme_bw() +
                         theme(legend.position="none") +
                         scale_fill_hue(l=45) +
                         scale_y_sqrt(breaks=hr_ticks_3d, limits=hr_range_3d),
                         #legend_labels + legend_style,
       width    = 27,
       height   = 21,
       units    = 'cm')

## job cputime consumption vs. queue
ggsave(filename = paste(plot_odir, paste('cctime_queue_scatter', o_suffix, '.png',sep=''),sep='/'),
       plot     = ggplot(job_data, aes(y=cctime/3600., x=queue)) +
                         geom_jitter(aes(color=cstat), alpha=0.5, size=1) + 
                         geom_boxplot(outlier.shape=NA, alpha=0.5) +
                         facet_wrap(~ cstat, ncol=1) +
                         ggtitle(paste('Job CPUtime Consumption', t_suffix, sep=' ')) +
                         xlab('queue') +
                         ylab('hours') +
                         theme_bw() +
                         theme(legend.position="none") +
                         scale_fill_hue(l=45) +
                         scale_y_sqrt(breaks=hr_ticks_3d, limits=hr_range_3d),
                         #legend_labels + legend_style,
       width    = 27,
       height   = 21,
       units    = 'cm')

## job cpu utilization fraction vs. queue
ggsave(filename = paste(plot_odir, paste('eff_cpu_queue_scatter', o_suffix, '.png',sep=''),sep='/'),
       plot     = ggplot(job_data, aes(y=100.*cctime/cwtime, x=queue)) +
                         geom_jitter(aes(color=cstat), alpha=0.5, size=1) + 
                         geom_boxplot(outlier.shape=NA, alpha=0.5) +
                         facet_wrap(~ cstat, ncol=1) +
                         ggtitle(paste('Job CPU Utilization Fraction', t_suffix, sep=' ')) +
                         xlab('queue') +
                         ylab('percent') +
                         theme_bw() +
                         theme(legend.position="none") +
                         scale_fill_hue(l=45) +
                         scale_y_continuous(breaks=frac_ticks, limits=c(0,100)),
                         #legend_labels + legend_style,
       width    = 27,
       height   = 21,
       units    = 'cm')

## job memory request vs. queue
ggsave(filename = paste(plot_odir, paste('rmem_queue_scatter', o_suffix, '.png',sep=''),sep='/'),
       plot     = ggplot(job_data, aes(y=rmem, x=queue)) +
                         geom_jitter(aes(color=cstat), alpha=0.5, size=1) + 
                         geom_boxplot(outlier.shape=NA, alpha=0.5) +
                         facet_wrap(~ cstat, ncol=1) +
                         ggtitle(paste('Job Memory Request', t_suffix, sep=' ')) +
                         xlab('queue') +
                         ylab('GB') +
                         theme_bw() +
                         theme(legend.position="none") +
                         scale_fill_hue(l=45) +
                         scale_y_sqrt(breaks=GB_ticks),
                         #legend_labels + legend_style,
       width    = 27,
       height   = 21,
       units    = 'cm')

## job memory consumption vs. queue
ggsave(filename = paste(plot_odir, paste('cmem_queue_scatter', o_suffix, '.png',sep=''),sep='/'),
       plot     = ggplot(job_data, aes(y=cmem, x=queue)) +
                         geom_jitter(aes(color=cstat), alpha=0.5, size=1) + 
                         geom_boxplot(outlier.shape=NA, alpha=0.5) +
                         facet_wrap(~ cstat, ncol=1) +
                         ggtitle(paste('Job Memory Consumption',t_suffix,sep=' ')) +
                         xlab('queue') +
                         ylab('GB') +
                         theme_bw() +
                         theme(legend.position="none") +
                         scale_fill_hue(l=45) +
                         scale_y_sqrt(breaks=GB_ticks),
                         #legend_labels + legend_style,
       width    = 27,
       height   = 21,
       units    = 'cm')

## job memory utilization fraction 
ggsave(filename = paste(plot_odir,paste('eff_mem_queue_scatter', o_suffix, '.png',sep=''),sep='/'),
       plot     = ggplot(job_data, aes(y=100.*cmem/rmem, x=queue)) +
                         geom_jitter(aes(color=cstat), alpha=0.5, size=1) + 
                         geom_boxplot(outlier.shape=NA, alpha=0.5) +
                         facet_wrap(~ cstat, ncol=1) +
                         ggtitle(paste('Job Memory Utilization Fraction',t_suffix,sep=' ')) +
                         xlab('queue') +
                         ylab('percent') +
                         theme_bw() +
                         theme(legend.position="none") +
                         scale_fill_hue(l=45) +
                         scale_y_continuous(breaks=frac_ticks, limits=c(0,100)),
                         #legend_labels + legend_style,
       width    = 27,
       height   = 21,
       units    = 'cm')

## correlation plots
## requested walltime vs. requested memory
ggsave(filename = paste(plot_odir,paste('rwtime_rmem_queue_scatter', o_suffix, '.png',sep=''),sep='/'),
       plot     = ggplot(job_data, aes(x=rwtime/3600., y=rmem)) +
                         geom_point(aes(color=cstat), alpha=0.5, size=1) +
                         facet_wrap(~ queue, ncol=3) +
                         ggtitle(paste('requested walltime vs. requested memory',t_suffix,sep=' ')) +
                         xlab('hours') +
                         ylab('GB') +
                         theme_bw() +
                         legend_labels + legend_style +
                         scale_fill_hue(l=45) +
                         scale_y_sqrt(breaks=GB_ticks) +
                         scale_x_sqrt(breaks=hr_ticks_3d),
       width    = 27,
       height   = 21,
       units    = 'cm')

## requested walltime vs. consumed memory
ggsave(filename = paste(plot_odir,paste('rwtime_cmem_queue_scatter', o_suffix, '.png',sep=''),sep='/'),
       plot     = ggplot(job_data, aes(x=rwtime/3600., y=cmem)) +
                         geom_point(aes(color=cstat), alpha=0.5, size=1) +
                         facet_wrap(~ queue, ncol=3) +
                         ggtitle(paste('requested walltime vs. consumed memory',t_suffix,sep=' ')) +
                         xlab('hours') +
                         ylab('GB') +
                         theme_bw() +
                         legend_labels + legend_style +
                         scale_fill_hue(l=45) +
                         scale_y_sqrt(breaks=GB_ticks) +
                         scale_x_sqrt(breaks=hr_ticks_3d),
       width    = 27,
       height   = 21,
       units    = 'cm')

## consumed walltime vs. requested memory
ggsave(filename = paste(plot_odir,paste('cwtime_rmem_queue_scatter', o_suffix, '.png',sep=''),sep='/'),
       plot     = ggplot(job_data, aes(x=cwtime/3600., y=rmem)) +
                         geom_point(aes(color=cstat), alpha=0.5, size=1) +
                         facet_wrap(~ queue, ncol=3) +
                         ggtitle(paste('consumed walltime vs. requested memory',t_suffix,sep=' ')) +
                         xlab('hours') +
                         ylab('GB') +
                         theme_bw() +
                         legend_labels + legend_style +
                         scale_fill_hue(l=45) +
                         scale_y_sqrt(breaks=GB_ticks) +
                         scale_x_sqrt(breaks=hr_ticks_3d),
       width    = 27,
       height   = 21,
       units    = 'cm')

## consumed walltime vs. consumed memory
ggsave(filename = paste(plot_odir,paste('cwtime_cmem_queue_scatter', o_suffix, '.png',sep=''),sep='/'),
       plot     = ggplot(job_data, aes(x=cwtime/3600., y=cmem)) +
                         geom_point(aes(color=cstat), alpha=0.5, size=1) +
                         facet_wrap(~ queue, ncol=3) +
                         ggtitle(paste('consumed walltime vs. consumed memory',t_suffix,sep=' ')) +
                         xlab('hours') +
                         ylab('GB') +
                         theme_bw() +
                         legend_labels + legend_style +
                         scale_fill_hue(l=45) +
                         scale_y_sqrt(breaks=GB_ticks) +
                         scale_x_sqrt(breaks=hr_ticks_3d),
       width    = 27,
       height   = 21,
       units    = 'cm')

## requested walltime vs. job waiting time
ggsave(filename = paste(plot_odir,paste('rwtime_qtime_queue_scatter', o_suffix, '.png',sep=''),sep='/'),
       plot     = ggplot(job_data, aes(x=rwtime/3600., y=1.0*(t_start-t_queue)/3600)) +
                         geom_point(aes(color=cstat), alpha=0.5, size=1) +
                         facet_wrap(~ queue, ncol=3) +
                         ggtitle(paste('requested walltime vs. job waiting time',t_suffix,sep=' ')) +
                         xlab('requested walltime (hours)') +
                         ylab('job waiting time (hours)') +
                         theme_bw() +
                         legend_labels + legend_style +
                         scale_fill_hue(l=45) +
                         scale_x_sqrt(breaks=hr_ticks_3d) +
                         scale_y_sqrt(breaks=hr_ticks_3d),
       width    = 27,
       height   = 21,
       units    = 'cm')

## requested memory vs. job waiting time
ggsave(filename = paste(plot_odir,paste('rmem_qtime_queue_scatter', o_suffix, '.png',sep=''),sep='/'),
       plot     = ggplot(job_data, aes(x=rmem, y=1.0*(t_start-t_queue)/3600)) +
                         geom_point(aes(color=cstat), alpha=0.5, size=1) +
                         facet_wrap(~ queue, ncol=3) +
                         ggtitle(paste('requested memory vs. job waiting time',t_suffix,sep=' ')) +
                         xlab('request memory (GB)') +
                         ylab('job waiting time (hours)') +
                         theme_bw() +
                         legend_labels + legend_style +
                         scale_fill_hue(l=45) +
                         scale_x_sqrt(breaks=GB_ticks) +
                         scale_y_sqrt(breaks=hr_ticks_3d),
       width    = 27,
       height   = 21,
       units    = 'cm')

## CPU utilisation vs. memory utilisation
ggsave(filename = paste(plot_odir,paste('effcpu_effmem_queue_scatter', o_suffix, '.png',sep=''),sep='/'),
       plot     = ggplot(job_data, aes(x=100.*cctime/cwtime, y=100.*cmem/rmem)) +
                         geom_point(aes(color=cstat), alpha=0.5, size=1) +
                         facet_wrap(~ queue, ncol=3) +
                         ggtitle(paste('CPU utilisation vs. memory utilisation',t_suffix,sep=' ')) +
                         xlab('cpu utilisation %') +
                         ylab('memory utilisation %') +
                         theme_bw() +
                         legend_labels + legend_style +
                         scale_fill_hue(l=45) +
                         scale_y_continuous(breaks=frac_ticks, limits=c(0,100)) +
                         scale_x_continuous(breaks=frac_ticks, limits=c(0,100)),
       width    = 27,
       height   = 21,
       units    = 'cm')