library(shiny)
library(bslib)
library(DBI)
library(odbc)
library(connectapi)
library(plotly)
library(dplyr)
library(DT)

# ── Data helpers ──────────────────────────────────────────────────────────────

fetch_snowflake <- function(access_token) {
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

  df <- dbGetQuery(conn, "SELECT * FROM SALES")
  names(df) <- toupper(names(df))
  df$SALE_DATE <- as.Date(df$SALE_DATE)
  df$MONTH <- format(df$SALE_DATE, "%Y-%m")
  df
}

fetch_databricks <- function(access_token) {
  conn <- dbConnect(
    odbc::odbc(),
    driver = "Databricks",
    Host = Sys.getenv("DATABRICKS_HOST"),
    Port = 443,
    HTTPPath = Sys.getenv("DATABRICKS_HTTP_PATH"),
    SSL = 1,
    ThriftTransport = 2,
    AuthMech = 11,
    Auth_Flow = 0,
    Auth_AccessToken = access_token
  )
  on.exit(dbDisconnect(conn))

  df <- dbGetQuery(conn, "
    SELECT
      t.dateTime,
      t.product,
      t.quantity,
      t.totalPrice,
      c.continent,
      c.country,
      f.name AS franchise_name
    FROM samples.bakehouse.sales_transactions t
    JOIN samples.bakehouse.sales_customers c
      ON t.customerID = c.customerID
    JOIN samples.bakehouse.sales_franchises f
      ON t.franchiseID = f.franchiseID
  ")
  names(df) <- tolower(names(df))
  df$datetime <- as.POSIXct(df$datetime)
  df$month <- format(df$datetime, "%Y-%m")
  df
}

# ── UI ────────────────────────────────────────────────────────────────────────

ui <- page_sidebar(
  title = "Multi-Source Analytics",
  sidebar = sidebar(
    width = 260,
    h3("Multi-Source Dashboard"),
    p(
      "Snowflake + Databricks via OAuth (connectapi)",
      style = "color: #6c757d; font-size: 0.85rem; margin: 0 0 1rem 0;"
    ),
    actionButton("load_data", "Refresh All Data", class = "btn-primary w-100"),
    tags$script(HTML(
      "setTimeout(function() { document.getElementById('load_data').click(); }, 500);"
    )),
    tags$hr(),
    tags$label("Snowflake Filters", style = "font-weight: 600;"),
    selectInput("sf_category", "Category", choices = "All", selected = "All"),
    selectInput("sf_region", "Region", choices = "All", selected = "All"),
    tags$hr(),
    tags$label("Databricks Filters", style = "font-weight: 600;"),
    selectInput("db_continent", "Continent", choices = "All", selected = "All"),
    selectInput("db_franchise", "Franchise", choices = "All", selected = "All")
  ),

  # Snowflake section
  h4("Snowflake - Sales", style = "margin-top: 0.5rem;"),
  layout_columns(
    value_box("SF Total Sales", textOutput("sf_total_sales"), theme = "primary", height = "150px"),
    value_box("SF Orders", textOutput("sf_total_orders"), theme = "info", height = "150px"),
    value_box("SF Avg Order", textOutput("sf_avg_order"), theme = "success", height = "150px"),
    col_widths = c(4, 4, 4)
  ),
  layout_columns(
    card(card_header("Sales by Category"), plotlyOutput("sf_chart_category", height = "400px")),
    card(card_header("Sales by Region"), plotlyOutput("sf_chart_region", height = "400px")),
    col_widths = c(6, 6)
  ),

  # Databricks section
  h4("Databricks - Bakehouse", style = "margin-top: 1.5rem;"),
  layout_columns(
    value_box("DB Revenue", textOutput("db_total_revenue"), theme = "primary", height = "150px"),
    value_box("DB Orders", textOutput("db_total_orders"), theme = "info", height = "150px"),
    value_box("DB Franchises", textOutput("db_franchise_count"), theme = "warning", height = "150px"),
    col_widths = c(4, 4, 4)
  ),
  layout_columns(
    card(card_header("Revenue by Franchise"), plotlyOutput("db_chart_franchise", height = "400px")),
    card(card_header("Revenue by Continent"), plotlyOutput("db_chart_continent", height = "400px")),
    col_widths = c(6, 6)
  ),

  # Combined trend
  layout_columns(
    card(card_header("Monthly Trends - Both Sources"), plotlyOutput("combined_trend", height = "450px")),
    col_widths = 12
  )
)

# ── Server ────────────────────────────────────────────────────────────────────

server <- function(input, output, session) {
  sf_data <- reactiveVal(NULL)
  db_data <- reactiveVal(NULL)

  observeEvent(input$load_data, {
    session_token <- session$request$HTTP_POSIT_CONNECT_USER_SESSION_TOKEN
    if (is.null(session_token) || session_token == "") {
      showNotification("No session token found. Deploy this app on Posit Connect.",
                       type = "error")
      return()
    }

    client <- connectapi::connect()

    # Discover integrations via content associations
    current_content <- connectapi::content_item(client, Sys.getenv("CONNECT_CONTENT_GUID"))
    associations <- connectapi::get_associations(current_content)

    sf_assoc <- Filter(function(a) a$oauth_integration_template == "snowflake", associations)
    db_assoc <- Filter(function(a) a$oauth_integration_template == "databricks", associations)

    if (length(sf_assoc) == 0 || length(db_assoc) == 0) {
      showNotification(
        paste0("Missing association: snowflake=", length(sf_assoc) > 0,
               ", databricks=", length(db_assoc) > 0),
        type = "error"
      )
      return()
    }

    sf_guid <- sf_assoc[[1]]$oauth_integration_guid
    db_guid <- db_assoc[[1]]$oauth_integration_guid

    # Fetch Snowflake
    tryCatch({
      sf_creds <- connectapi::get_oauth_credentials(client, session_token, audience = sf_guid)
      df_sf <- fetch_snowflake(sf_creds$access_token)
      sf_data(df_sf)

      updateSelectInput(session, "sf_category",
                        choices = c("All", sort(unique(df_sf$CATEGORY))), selected = "All")
      updateSelectInput(session, "sf_region",
                        choices = c("All", sort(unique(df_sf$REGION))), selected = "All")

      showNotification(paste("Snowflake:", nrow(df_sf), "rows"), type = "message")
    }, error = function(e) {
      showNotification(paste("Snowflake error:", conditionMessage(e)),
                       type = "error", duration = 10)
    })

    # Fetch Databricks
    tryCatch({
      db_creds <- connectapi::get_oauth_credentials(client, session_token, audience = db_guid)
      df_db <- fetch_databricks(db_creds$access_token)
      db_data(df_db)

      updateSelectInput(session, "db_continent",
                        choices = c("All", sort(unique(df_db$continent))), selected = "All")
      updateSelectInput(session, "db_franchise",
                        choices = c("All", sort(unique(df_db$franchise_name))), selected = "All")

      showNotification(paste("Databricks:", nrow(df_db), "rows"), type = "message")
    }, error = function(e) {
      showNotification(paste("Databricks error:", conditionMessage(e)),
                       type = "error", duration = 10)
    })
  })

  # ── Snowflake filtered data ──

  sf_filtered <- reactive({
    df <- sf_data()
    if (is.null(df)) return(NULL)
    if (input$sf_category != "All") df <- df[df$CATEGORY == input$sf_category, ]
    if (input$sf_region != "All") df <- df[df$REGION == input$sf_region, ]
    df
  })

  output$sf_total_sales <- renderText({
    df <- sf_filtered()
    if (is.null(df)) return("--")
    paste0("$", formatC(sum(df$TOTAL_AMOUNT), format = "f", digits = 2, big.mark = ","))
  })

  output$sf_total_orders <- renderText({
    df <- sf_filtered()
    if (is.null(df)) return("--")
    formatC(nrow(df), format = "d", big.mark = ",")
  })

  output$sf_avg_order <- renderText({
    df <- sf_filtered()
    if (is.null(df) || nrow(df) == 0) return("--")
    paste0("$", formatC(mean(df$TOTAL_AMOUNT), format = "f", digits = 2, big.mark = ","))
  })

  output$sf_chart_category <- renderPlotly({
    df <- sf_filtered()
    if (is.null(df)) return(plot_ly() |> layout(title = "Loading..."))
    agg <- df |> group_by(CATEGORY) |> summarise(total = sum(TOTAL_AMOUNT), .groups = "drop")
    plot_ly(agg, x = ~CATEGORY, y = ~total, color = ~CATEGORY, type = "bar") |>
      layout(xaxis = list(title = "Category"), yaxis = list(title = "Sales ($)"))
  })

  output$sf_chart_region <- renderPlotly({
    df <- sf_filtered()
    if (is.null(df)) return(plot_ly() |> layout(title = "Loading..."))
    agg <- df |> group_by(REGION) |> summarise(total = sum(TOTAL_AMOUNT), .groups = "drop")
    plot_ly(agg, labels = ~REGION, values = ~total, type = "pie")
  })

  # ── Databricks filtered data ──

  db_filtered <- reactive({
    df <- db_data()
    if (is.null(df)) return(NULL)
    if (input$db_continent != "All") df <- df[df$continent == input$db_continent, ]
    if (input$db_franchise != "All") df <- df[df$franchise_name == input$db_franchise, ]
    df
  })

  output$db_total_revenue <- renderText({
    df <- db_filtered()
    if (is.null(df)) return("--")
    paste0("$", formatC(sum(df$totalprice), format = "f", digits = 2, big.mark = ","))
  })

  output$db_total_orders <- renderText({
    df <- db_filtered()
    if (is.null(df)) return("--")
    formatC(nrow(df), format = "d", big.mark = ",")
  })

  output$db_franchise_count <- renderText({
    df <- db_filtered()
    if (is.null(df)) return("--")
    as.character(length(unique(df$franchise_name)))
  })

  output$db_chart_franchise <- renderPlotly({
    df <- db_filtered()
    if (is.null(df)) return(plot_ly() |> layout(title = "Loading..."))
    agg <- df |>
      group_by(franchise_name) |>
      summarise(revenue = sum(totalprice), .groups = "drop") |>
      arrange(revenue)
    agg$franchise_name <- factor(agg$franchise_name, levels = agg$franchise_name)
    plot_ly(agg, x = ~revenue, y = ~franchise_name, type = "bar", orientation = "h") |>
      layout(xaxis = list(title = "Revenue ($)"), yaxis = list(title = "Franchise"))
  })

  output$db_chart_continent <- renderPlotly({
    df <- db_filtered()
    if (is.null(df)) return(plot_ly() |> layout(title = "Loading..."))
    agg <- df |> group_by(continent) |> summarise(revenue = sum(totalprice), .groups = "drop")
    plot_ly(agg, labels = ~continent, values = ~revenue, type = "pie")
  })

  # ── Combined trend ──

  output$combined_trend <- renderPlotly({
    df_sf <- sf_filtered()
    df_db <- db_filtered()
    if (is.null(df_sf) && is.null(df_db)) {
      return(plot_ly() |> layout(title = "Loading..."))
    }

    p <- plot_ly()
    if (!is.null(df_sf)) {
      agg_sf <- df_sf |> group_by(MONTH) |> summarise(total = sum(TOTAL_AMOUNT), .groups = "drop") |> arrange(MONTH)
      p <- p |> add_trace(data = agg_sf, x = ~MONTH, y = ~total, type = "scatter",
                          mode = "lines+markers", name = "Snowflake Sales")
    }
    if (!is.null(df_db)) {
      agg_db <- df_db |> group_by(month) |> summarise(total = sum(totalprice), .groups = "drop") |> arrange(month)
      p <- p |> add_trace(data = agg_db, x = ~month, y = ~total, type = "scatter",
                          mode = "lines+markers", name = "Databricks Revenue")
    }
    p |> layout(xaxis = list(title = "Month"), yaxis = list(title = "Amount ($)"))
  })
}

shinyApp(ui, server)
