library(shiny)
library(bslib)
library(DBI)
library(odbc)
library(connectapi)
library(plotly)
library(dplyr)
library(DT)

# ── Data helpers ──────────────────────────────────────────────────────────────

fetch_data <- function(access_token) {
  conn <- dbConnect(
    odbc::odbc(),
    driver = "Snowflake",
    Server = paste0(Sys.getenv("SNOWFLAKE_ACCOUNT"), ".snowflakecomputing.com"),
    Database = Sys.getenv("SNOWFLAKE_DATABASE"),
    Schema = Sys.getenv("SNOWFLAKE_SCHEMA"),
    Warehouse = Sys.getenv("SNOWFLAKE_WAREHOUSE"),
    Authenticator = "oauth",
    Token = access_token
  )
  on.exit(dbDisconnect(conn))

  dbGetQuery(conn, "SELECT * FROM SALES")
}

# ── UI ────────────────────────────────────────────────────────────────────────

ui <- page_sidebar(
  title = "Snowflake Sales Dashboard",
  sidebar = sidebar(
    width = 250,
    actionButton("load_data", "Refresh Data", class = "btn-primary w-100"),
    tags$script(HTML(
      "setTimeout(function() { document.getElementById('load_data').click(); }, 500);"
    )),
    selectInput("category", "Category", choices = "All", selected = "All"),
    selectInput("region", "Region", choices = "All", selected = "All")
  ),

  layout_columns(
    value_box("Total Sales", textOutput("total_sales"), theme = "primary"),
    value_box("Total Orders", textOutput("total_orders"), theme = "info"),
    value_box("Avg Order Value", textOutput("avg_order"), theme = "success"),
    col_widths = c(4, 4, 4)
  ),
  layout_columns(
    card(card_header("Sales by Category"), plotlyOutput("chart_category")),
    card(card_header("Sales by Region"), plotlyOutput("chart_region")),
    col_widths = c(6, 6)
  ),
  layout_columns(
    card(card_header("Monthly Sales Trend"), plotlyOutput("chart_trend")),
    col_widths = 12
  ),
  card(
    card_header("Sales Data"),
    DTOutput("sales_table")
  )
)

# ── Server ────────────────────────────────────────────────────────────────────

server <- function(input, output, session) {
  raw_data <- reactiveVal(NULL)

  observeEvent(input$load_data, {
    tryCatch({
      session_token <- session$request$HTTP_POSIT_CONNECT_USER_SESSION_TOKEN
      if (is.null(session_token) || session_token == "") {
        showNotification("No session token found. Deploy this app on Posit Connect.",
                         type = "error")
        return()
      }

      client <- connectapi::connect()
      credentials <- connectapi::get_oauth_credentials(client, session_token)
      access_token <- credentials$access_token

      df <- fetch_data(access_token)
      names(df) <- toupper(names(df))
      df$SALE_DATE <- as.Date(df$SALE_DATE)
      df$MONTH <- format(df$SALE_DATE, "%Y-%m")

      raw_data(df)

      categories <- c("All", sort(unique(df$CATEGORY)))
      regions <- c("All", sort(unique(df$REGION)))
      updateSelectInput(session, "category", choices = categories, selected = "All")
      updateSelectInput(session, "region", choices = regions, selected = "All")

      showNotification(paste("Loaded", nrow(df), "rows from Snowflake"), type = "message")
    }, error = function(e) {
      showNotification(paste("Error:", conditionMessage(e)), type = "error", duration = 10)
    })
  })

  filtered_data <- reactive({
    df <- raw_data()
    if (is.null(df)) return(NULL)
    if (input$category != "All") {
      df <- df[df$CATEGORY == input$category, ]
    }
    if (input$region != "All") {
      df <- df[df$REGION == input$region, ]
    }
    df
  })

  output$total_sales <- renderText({
    df <- filtered_data()
    if (is.null(df)) return("--")
    paste0("$", formatC(sum(df$TOTAL_AMOUNT), format = "f", digits = 2, big.mark = ","))
  })

  output$total_orders <- renderText({
    df <- filtered_data()
    if (is.null(df)) return("--")
    formatC(nrow(df), format = "d", big.mark = ",")
  })

  output$avg_order <- renderText({
    df <- filtered_data()
    if (is.null(df) || nrow(df) == 0) return("--")
    paste0("$", formatC(mean(df$TOTAL_AMOUNT), format = "f", digits = 2, big.mark = ","))
  })

  output$chart_category <- renderPlotly({
    df <- filtered_data()
    if (is.null(df)) return(plot_ly() |> layout(title = "Loading..."))
    agg <- df |>
      group_by(CATEGORY) |>
      summarise(total = sum(TOTAL_AMOUNT), .groups = "drop")
    plot_ly(agg, x = ~CATEGORY, y = ~total, color = ~CATEGORY, type = "bar") |>
      layout(xaxis = list(title = "Category"), yaxis = list(title = "Total Sales ($)"))
  })

  output$chart_region <- renderPlotly({
    df <- filtered_data()
    if (is.null(df)) return(plot_ly() |> layout(title = "Loading..."))
    agg <- df |>
      group_by(REGION) |>
      summarise(total = sum(TOTAL_AMOUNT), .groups = "drop")
    plot_ly(agg, labels = ~REGION, values = ~total, type = "pie") |>
      layout(showlegend = TRUE)
  })

  output$chart_trend <- renderPlotly({
    df <- filtered_data()
    if (is.null(df)) return(plot_ly() |> layout(title = "Loading..."))
    agg <- df |>
      group_by(MONTH) |>
      summarise(total = sum(TOTAL_AMOUNT), .groups = "drop") |>
      arrange(MONTH)
    plot_ly(agg, x = ~MONTH, y = ~total, type = "scatter", mode = "lines+markers") |>
      layout(xaxis = list(title = "Month"), yaxis = list(title = "Total Sales ($)"))
  })

  output$sales_table <- renderDT({
    df <- filtered_data()
    if (is.null(df)) return(datatable(data.frame()))
    display <- df |>
      select(SALE_DATE, PRODUCT_NAME, CATEGORY, QUANTITY, UNIT_PRICE, TOTAL_AMOUNT, REGION, CUSTOMER_NAME) |>
      mutate(SALE_DATE = format(SALE_DATE, "%Y-%m-%d"))
    datatable(display, filter = "top", options = list(pageLength = 15))
  })
}

shinyApp(ui, server)
