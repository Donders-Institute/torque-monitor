library(shiny)

# Define UI for miles per gallon application
shinyUI(bootstrapPage(

    # Application title
    headerPanel("Job statistics"),
 
#    sidebarPanel(
#        selectInput("view", "Select view:",
#                    list("Job waiting time" = "qTime"))
#    ),
 
    mainPanel(
        verbatimTextOutput("summary"),
        plotOutput('qTime_queue_scatter')
#        plotOutput('qTime_rwtime_scatter'),
#        plotOutput('qTime_rmem_scatter')
    )
))
