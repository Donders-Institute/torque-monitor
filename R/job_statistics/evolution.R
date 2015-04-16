library('ggplot2')
library('grid')
library('RSQLite')
library('scales')

args <- commandArgs(TRUE)

if ( length(args) < 1 ) {
    stop('require DB_TOPDIR. Usage: Rscript evolution.R <DB_TOPDIR> [<NUM_OF_MONTHS>]')
}

# load data from SQLite
db_dpath  <- args[1]

## construct a pattern as 'mm_trackTorqueJobs_201402.db' if time_month is given
## otherwise, 'mm_trackTorqueJobs_[[:digit:]]*.db'
pattern  <- 'mm_trackTorqueJobs_[[:digit:]]*.db'

db_files  <- sort(list.files(path=db_dpath, pattern=pattern), decreasing = TRUE)

if ( length( db_files ) == 0 ) {
    stop('No data files.')
}

if ( length(args) == 2 ) {
    db_files <- db_files[1: min(c(strtoi(args[2]),length(db_files)))]
}

drv <- dbDriver('SQLite')

# initialise the data frame to store statistical data in question
#df_titles <- c('month','queue',
#               'rwtime_m', 'rwtime_u','rwtime_d','rwtime_min','rwtime_max',
#               'cwtime_m','cwtime_u','cwtime_d','cwtime_min','cwtime_max',
#               'rmem_m','rmem_u','rmem_d','rmem_min','rmem_max',
#               'cmem_m','cmem_u','cmem_d','cmem_min','cmem_max')
#
#df <- data.frame(matrix(ncol=length(df_titles), nrow=0),
#                 check.names = FALSE,
#                 stringsAsFactors = FALSE)
#
#names(df) <- df_titles
#

df <- data.frame('month' = character(0),
                 'queue' = character(0),
                 'njobs' = numeric(0),
                 'nusers' = numeric(0),
                 'rwtime_m' = numeric(0),
                 'rwtime_u' = numeric(0),
                 'rwtime_d' = numeric(0),
                 'rwtime_min' = numeric(0),
                 'rwtime_max' = numeric(0),
                 'rctime_m' = numeric(0),
                 'rctime_u' = numeric(0),
                 'rctime_d' = numeric(0),
                 'rctime_min' = numeric(0),
                 'rctime_max' = numeric(0),
                 'rmem_m' = numeric(0),
                 'rmem_u' = numeric(0),
                 'rmem_d' = numeric(0),
                 'rmem_min' = numeric(0),
                 'rmem_max' = numeric(0),
                 'cmem_m' = numeric(0),
                 'cmem_u' = numeric(0),
                 'cmem_d' = numeric(0),
                 'cmem_min' = numeric(0),
                 'cmem_max' = numeric(0))

# define quantile/percentile points for (median, low, up, min, max)
qpts <- c(0.5, 0.25, 0.75, 0, 1)

# queue name of everything
qname_everything = 'all'

# each db file contains jobs submitted within the month
for (f in db_files) {
    db_month   <- as.Date(paste(sub('.db','',sub('mm_trackTorqueJobs_','',f)),"01"),"%Y%m%d")
    #db_month   <- sub('.db','',sub('mm_trackTorqueJobs_','',f))
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
        # gets statistics of the month
        #  - everything together
        rwtime_stat <- quantile(t_job_data$rwtime / 3600., probs = qpts, names=FALSE, na.rm = TRUE)
        cwtime_stat <- quantile(t_job_data$cwtime / 3600., probs = qpts, names=FALSE, na.rm = TRUE)
        rmem_stat <- quantile(t_job_data$rmem, probs = qpts, names=FALSE, na.rm = TRUE)
        cmem_stat <- quantile(t_job_data$cmem, probs = qpts, names=FALSE, na.rm = TRUE)

        sd_r <- subset(t_job_data, ! is.na(rmem) & rmem > 0 )
        mem_eff_stat <- quantile(100 * sd_r$cmem / sd_r$rmem, probs = qpts, names=FALSE, na.rm = TRUE)

        df <- rbind(df, data.frame(month = db_month, queue = qname_everything,
                                   njobs = nrow(t_job_data),
                                   nusers = length(unique(t_job_data$uid)),
                                   rwtime_m = rwtime_stat[1],
                                   rwtime_d = rwtime_stat[2],
                                   rwtime_u = rwtime_stat[3],
                                   rwtime_min = rwtime_stat[4],
                                   rwtime_max = rwtime_stat[5],
                                   cwtime_m = cwtime_stat[1],
                                   cwtime_d = cwtime_stat[2],
                                   cwtime_u = cwtime_stat[3],
                                   cwtime_min = cwtime_stat[4],
                                   cwtime_max = cwtime_stat[5],
                                   rmem_m = rmem_stat[1],
                                   rmem_d = rmem_stat[2],
                                   rmem_u = rmem_stat[3],
                                   rmem_min = rmem_stat[4],
                                   rmem_max = rmem_stat[5],
                                   cmem_m = cmem_stat[1],
                                   cmem_d = cmem_stat[2],
                                   cmem_u = cmem_stat[3],
                                   cmem_min = cmem_stat[4],
                                   cmem_max = cmem_stat[5],
                                   mem_eff_m = mem_eff_stat[1],
                                   mem_eff_d = mem_eff_stat[2],
                                   mem_eff_u = mem_eff_stat[3],
                                   mem_eff_min = mem_eff_stat[4],
                                   mem_eff_max = mem_eff_stat[5]))

        #  - catagorized by job queue
        for (q in unique(t_job_data$queue)) {
            sd <- subset(t_job_data, queue == q)
            rwtime_stat <- quantile(sd$rwtime / 3600., probs = qpts, names=FALSE, na.rm = TRUE)
            cwtime_stat <- quantile(sd$cwtime / 3600., probs = qpts, names=FALSE, na.rm = TRUE)
            rmem_stat <- quantile(sd$rmem, probs = qpts, names=FALSE, na.rm = TRUE)
            cmem_stat <- quantile(sd$cmem, probs = qpts, names=FALSE, na.rm = TRUE)

            sd_r <- subset(sd, ! is.na(rmem) & rmem > 0 )

            mem_eff_stat <- quantile(100 * sd_r$cmem / sd_r$rmem, probs = qpts, names=FALSE, na.rm = TRUE)
            df <- rbind(df, data.frame(month = db_month, queue = q,
                                       njobs = nrow(sd),
                                       nusers = length(unique(sd$uid)),
                                       rwtime_m = rwtime_stat[1],
                                       rwtime_d = rwtime_stat[2],
                                       rwtime_u = rwtime_stat[3],
                                       rwtime_min = rwtime_stat[4],
                                       rwtime_max = rwtime_stat[5],
                                       cwtime_m = cwtime_stat[1],
                                       cwtime_d = cwtime_stat[2],
                                       cwtime_u = cwtime_stat[3],
                                       cwtime_min = cwtime_stat[4],
                                       cwtime_max = cwtime_stat[5],
                                       rmem_m = rmem_stat[1],
                                       rmem_d = rmem_stat[2],
                                       rmem_u = rmem_stat[3],
                                       rmem_min = rmem_stat[4],
                                       rmem_max = rmem_stat[5],
                                       cmem_m = cmem_stat[1],
                                       cmem_d = cmem_stat[2],
                                       cmem_u = cmem_stat[3],
                                       cmem_min = cmem_stat[4],
                                       cmem_max = cmem_stat[5],
                                       mem_eff_m = mem_eff_stat[1],
                                       mem_eff_d = mem_eff_stat[2],
                                       mem_eff_u = mem_eff_stat[3],
                                       mem_eff_min = mem_eff_stat[4],
                                       mem_eff_max = mem_eff_stat[5]))
        }
    }
}
dbUnloadDriver(drv)

print(df)
# output data frame to CSV file
write.csv(df, file='evolution.csv')

# plotting monthly evolution
plot_odir <- paste(db_dpath, 'plots', sep='/')
hr_ticks_3d <- c(0,2,4,12,24,48,72)
hr_ticks_5d <- append(hr_ticks_3d, c(96,120))
GB_ticks    <- c(0,4,8,16,32,64,128,256)
frac_ticks  <- seq(0,100,10)

hr_range_3d <- c(0,72)
hr_range_5d <- c(0,120)
GB_range    <- c(0,256)

date_label_fmt <- "%b"

if (length( db_files ) > 11 ) {
    date_label_fmt <- "'%y-%m"
}

# number of unique users per queue
ggsave(filename = paste(plot_odir, 'nusers_queue_evolution_monthly.png', sep='/'),
       plot     = ggplot(df[df$queue != qname_everything, ], aes(x=month, y=nusers, fill=queue, order=queue)) +
                         geom_bar(stat='identity', position='stack') +
                         ggtitle('Number of unique users counted by queue') +
                         xlab('month') +
                         ylab('count') +
                         theme_bw() +
                         theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
                         scale_fill_hue(l=45) +
                         scale_x_date(labels = date_format(date_label_fmt), breaks = date_breaks("month")),
##                         legend_labels + legend_style,
       width    = 27,
       height   = 15,
       units    = 'cm')

# number of unique users
ggsave(filename = paste(plot_odir, 'nusers_all_evolution_monthly.png', sep='/'),
       plot     = ggplot(df[df$queue == qname_everything, ], aes(x=month, y=nusers, fill=queue, order=queue)) +
                         geom_bar(stat='identity', position='stack') +
                         ggtitle('Number of unique users') +
                         xlab('month') +
                         ylab('count') +
                         theme_bw() +
                         theme(legend.position="none", axis.text.x = element_text(angle = 45, hjust = 1)) +
                         scale_fill_hue(l=45) +
                         scale_x_date(labels = date_format(date_label_fmt), breaks = date_breaks("month")),
##                         legend_labels + legend_style,
       width    = 27,
       height   = 15,
       units    = 'cm')

# number of jobs per queue
ggsave(filename = paste(plot_odir, 'njobs_evolution_monthly.png', sep='/'),
       plot     = ggplot(df[df$queue != qname_everything, ], aes(x=month, y=njobs, fill=queue, order=queue)) +
                         geom_bar(stat='identity', position='stack') +
                         ggtitle('Number of jobs') +
                         xlab('month') +
                         ylab('count') +
                         theme_bw() +
                         theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
                         scale_fill_hue(l=45) +
                         scale_x_date(labels = date_format(date_label_fmt), breaks = date_breaks("month")),
##                         legend_labels + legend_style,
       width    = 27,
       height   = 15,
       units    = 'cm')

# requested memory
ggsave(filename = paste(plot_odir, 'rmem_evolution_monthly.png', sep='/'),
       plot     = ggplot(df, aes(x=month, y=rmem_m, group=queue, fill=queue)) +
                         geom_bar(stat='identity') +
                         geom_errorbar(aes(ymax=rmem_u, ymin=rmem_d)) +
       #                  geom_ribbon(aes(ymax=rmem_u, ymin=rmem_d)) +
       #                  geom_line(aes(y=rmem_m)) +
                         facet_wrap(~ queue, ncol=3) +
                         ggtitle('Requested memory') +
                         xlab('month') +
                         ylab('GB') +
                         theme_bw() +
                         theme(legend.position="none", axis.text.x = element_text(angle = 45, hjust = 1)) +
                         scale_x_date(labels = date_format(date_label_fmt), breaks = date_breaks("month")) +
			             scale_y_continuous(breaks=GB_ticks),
##                         legend_labels + legend_style,
       width    = 27,
       height   = 21,
       units    = 'cm')

# consumed memory
ggsave(filename = paste(plot_odir, 'cmem_evolution_monthly.png', sep='/'),
       plot     = ggplot(df, aes(x=month, y=cmem_m, group=queue, fill=queue)) +
                         geom_bar(stat='identity') +
                         geom_errorbar(aes(ymax=cmem_u, ymin=cmem_d)) +
       #                  geom_ribbon(aes(ymax=cmem_u, ymin=cmem_d)) +
       #                  geom_line(aes(y=cmem_m)) +
                         facet_wrap(~ queue, ncol=3) +
                         ggtitle('Consumed memory') +
                         xlab('month') +
                         ylab('GB') +
                         theme_bw() +
                         theme(legend.position="none", axis.text.x = element_text(angle = 45, hjust = 1)) +
                         scale_x_date(labels = date_format(date_label_fmt), breaks = date_breaks("month")) +
			             scale_y_continuous(breaks=GB_ticks),
##                         legend_labels + legend_style,
       width    = 27,
       height   = 21,
       units    = 'cm')

# memory utilisation fraction
ggsave(filename = paste(plot_odir, 'eff_mem_evolution_monthly.png', sep='/'),
       plot     = ggplot(df, aes(x=month, y=mem_eff_m, group=queue, fill=queue)) +
                         geom_bar(stat='identity') +
                         geom_errorbar(aes(ymax=mem_eff_u, ymin=mem_eff_d)) +
       #                  geom_ribbon(aes(ymax=mem_eff_u, ymin=mem_eff_d)) +
       #                  geom_line(aes(y=mem_eff_m)) +
                         facet_wrap(~ queue, ncol=3) +
                         ggtitle('Memory utilisation fraction') +
                         xlab('month') +
                         ylab('%') +
                         theme_bw() +
                         theme(legend.position="none", axis.text.x = element_text(angle = 45, hjust = 1)) +
                         scale_x_date(labels = date_format(date_label_fmt), breaks = date_breaks("month")) +
			             scale_y_continuous(breaks=frac_ticks, limits=c(0,110)),
##                         legend_labels + legend_style,
       width    = 27,
       height   = 21,
       units    = 'cm')

# requested walltime
ggsave(filename = paste(plot_odir, 'rwtime_evolution_monthly.png', sep='/'),
       plot     = ggplot(df, aes(x=month, y=rwtime_m, group=queue, fill=queue)) +
                         geom_bar(stat='identity') +
                         geom_errorbar(aes(ymax=rwtime_u, ymin=rwtime_d)) +
       #                  geom_ribbon(aes(ymax=rwtime_u, ymin=rwtime_d)) +
       #                  geom_line(aes(y=rwtime_m)) +
                         facet_wrap(~ queue, ncol=3) +
                         ggtitle('Requested walltime') +
                         xlab('month') +
                         ylab('hour') +
                         theme_bw() +
                         theme(legend.position="none", axis.text.x = element_text(angle = 45, hjust = 1)) +
                         scale_x_date(labels = date_format(date_label_fmt), breaks = date_breaks("month")) +
			             scale_y_continuous(breaks=hr_ticks_3d),
##                         legend_labels + legend_style,
       width    = 27,
       height   = 21,
       units    = 'cm')

# consumed walltime
ggsave(filename = paste(plot_odir, 'cwtime_evolution_monthly.png', sep='/'),
       plot     = ggplot(df, aes(x=month, y=cwtime_m, group=queue, fill=queue)) +
                         geom_bar(stat='identity') +
                         geom_errorbar(aes(ymax=cwtime_u, ymin=cwtime_d)) +
       #                  geom_ribbon(aes(ymax=cwtime_u, ymin=cwtime_d)) +
       #                  geom_line(aes(y=cwtime_m)) +
                         facet_wrap(~ queue, ncol=3) +
                         ggtitle('Consumed walltime') +
                         xlab('month') +
                         ylab('hour') +
                         theme_bw() +
                         theme(legend.position="none", axis.text.x = element_text(angle = 45, hjust = 1)) +
                         scale_x_date(labels = date_format(date_label_fmt), breaks = date_breaks("month")) +
			             scale_y_continuous(breaks=hr_ticks_3d),
##                         legend_labels + legend_style,
       width    = 27,
       height   = 21,
       units    = 'cm')
