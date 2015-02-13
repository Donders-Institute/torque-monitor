library('shiny')
library('ggplot2')
library('RSQLite')

# load data from SQLite
db_dpath <- '/home/hclee/fs_home/projects/cluster_monitor/stat/db'
db_files <- list.files(path=db_dpath, pattern='mm_trackTorqueJobs_[[:digit:]]*.db') 
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
                                       WHERE cwtime IS NOT NULL AND cctime < cwtime')
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
    df <- data.frame('queue'                = sd$queue,
                     'waiting time(sec)'    = sd$t_start - sd$t_queue,
                     'memory request(GB)'   = sd$rmem,
                     'walltime request(hr)' = sd$rwtime / 3600.)
    return(df)
}

# Define server logic
shinyServer(function(input, output) {

    ## making summary data
    #output$summary_matlab <- renderPrint( {summary( getSubDataframeForSummary( job_data, 'matlab'      ) ) }) 
    #output$summary_batch  <- renderPrint( {summary( getSubDataframeForSummary( job_data, 'batch'       ) ) })
    #output$summary_inter  <- renderPrint( {summary( getSubDataframeForSummary( job_data, 'interactive' ) ) })
    #output$summary_short  <- renderPrint( {summary( getSubDataframeForSummary( job_data, 'short'       ) ) })
    #output$summary_vshort <- renderPrint( {summary( getSubDataframeForSummary( job_data, 'veryshort'   ) ) })
    #output$summary_long   <- renderPrint( {summary( getSubDataframeForSummary( job_data, 'long'        ) ) })
    #output$summary_vlong  <- renderPrint( {summary( getSubDataframeForSummary( job_data, 'verylong'    ) ) })
    output$summary <- renderPrint( {summary( getSubDataframeForSummary( job_data, 'all' ) ) })

    ## making plots 
    output$qTime_queue_scatter <- renderPlot({
        p <- ggplot(job_data, aes(y=t_start-t_queue, x=queue), color=queue) +
               geom_jitter(aes(color=queue)) + 
               geom_boxplot(color='red',outlier.shape = NA) +
               xlab('queue') +
               ylab('waiting time (sec.)') +
               scale_y_continuous(breaks=seq(0, 5*86400, 43200)) +
               theme(legend.position = "none")
        print(p)
    })

#    output$qTime_rmem_scatter <- renderPlot({
#        p <- ggplot(job_data, aes(x=rmem, y=t_start-t_queue, color=uid)) +
#               geom_point(shape=1) +
#               facet_wrap(~ queue, ncol=2) +
#               scale_colour_hue(l=50) +
#               xlab('memory request (GB)') +
#               ylab('waiting time (sec.)') +
#               scale_x_continuous(breaks=seq(0,     160,    16)) +
#               scale_y_continuous(breaks=seq(0, 5*86400, 43200)) +
#               theme(legend.position = "none")
#        print(p)
#    })

#    output$qTime_rwtime_scatter <- renderPlot({
#        p <- ggplot(job_data, aes(x=rwtime, y=t_start-t_queue, color=uid)) +
#               geom_point(shape=1) +
#               facet_wrap(~ queue, ncol=2) +
#               scale_colour_hue(l=50) +
#               xlab('walltime request (sec.)') +
#               ylab('waiting time (sec.)') +
#               scale_x_continuous(breaks=seq(0, 3*86400, 43200)) +
#               scale_y_continuous(breaks=seq(0, 5*86400, 43200)) +
#               theme(legend.position = "none")
#        print(p)
#    })

})
