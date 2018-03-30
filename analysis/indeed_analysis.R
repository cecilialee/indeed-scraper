library(tidyverse)
library(httr)
library(jsonlite)
library(tidytext)

# Load datd ===================================================================

# Download data from morph.io
api_url <- "https://api.morph.io/cecilialee/morph-scraper/data.json"
api_key <- Sys.getenv("API_KEY")

r <- GET(api_url, query = list(key = api_key, query = "select * from 'data'"))
c <- content(r, as = "text")

dshk <- fromJSON(c)

# Data wrangling ==============================================================

dshk <- dshk %>% 
  # Tibble
  as.tibble() %>% 
  # Select useful variables
  select(scrping_dt:ad_url) %>% 
  # Arrange by latest scraping datetime
  arrange(desc(scrping_dt)) %>% 
  # Filter duplicated ids
  distinct(ad_id_indeed, .keep_all = TRUE) %>% 
  # Wrangle scrping_dt
  mutate(scrping_dt = as.Date(scrping_dt)) %>%
  # Wrangle ad_post_dt_indeed
  mutate(ad_post_dt_indeed = scrping_dt - parse_number(ad_post_dt_indeed))

# Feature Engineering =========================================================

# Industry --------------------------------------------------------------------
companies <- dshk %>% distinct(ad_cie_indeed)
# TODO: Manual entry company names

# Job title -------------------------------------------------------------------
roles <- c("data scientist", "scientist", "analyst", "programmer", 
           "developer", "engineer")
seniority <- c("intern", "assistant", "junior", "senior", "manager", 
               "principal", "lead", "head", "chief", "director")

# Text mining =================================================================

# ad_jobdes_indeed #

dshk_tokens <- dshk %>%
  as.tibble() %>% 
  unnest_tokens(word, ad_jobdes_indeed)

stop_words0 <- stop_words %>% 
  filter(word != "r")

my_stop_words <- c("hong", "kong", "client", "clients", "including", "role")

dshk_tokens <- dshk_tokens %>% 
  anti_join(stop_words0) %>% 
  filter(!word %in% my_stop_words)

dshk_tokens %>% 
  count(word, sort = TRUE) %>% 
  filter(n > 200) %>% 
  mutate(word = reorder(word, n)) %>% 
  ggplot(aes(word, n)) +
  geom_col() +
  xlab(NULL) + 
  coord_flip()

# Mining skills
skills <- c("python", "r", "sas", "spark", "pandas", "matplotlib", "pyspark",
            "shiny", "sql", "tableau", "mongodb", "javascript", "c", "ggplot",
            "plotly", "d3", "perl", "java")

dshk_skills_tokens <- dshk_tokens %>% filter(word %in% skills)

dshk_skills_tokens %>% 
  ggplot() + geom_histogram(aes(x = word), stat = "count")

# Mining degrees
dshk_tokens %>% 
  count(word) %>% 
  filter(word == "degree") %>% 
  View()

# ---------- #

dshk_bigrams <- dshk %>% 
  select(ad_jobtitle_indeed, ad_cie_indeed, ad_jobdes_indeed) %>% 
  unnest_tokens(bigram, ad_jobdes_indeed, token = "ngrams", n = 2)

dshk_bigrams %>% 
  separate(bigram, c("word1", "word2"), sep = " ") %>% 
  filter(!word1 %in% stop_words$word) %>% 
  filter(!word2 %in% stop_words$word) %>% 
  filter(!word1 == "hong", !word2 == "kong") %>% 
  filter(!word1 == "kpmg", !word2 == "international") %>% 
  filter(!word1 == "recruitment", !word2 == "purposes") %>% 
  filter(!word1 == "sas", !word2 == "spss") %>% 
  filter(!word1 == "expected", !word2 == "salary") %>% 
  filter(!word1 == "skills", !word2 == "including") %>% 
  filter(!word1 == "fastest", !word2 == "growing") %>% 
  unite(bigram, word1, word2, sep = " ") %>% 
  count(bigram, sort = TRUE) %>% 
  filter(n > 30) %>% 
  ggplot(aes(x = reorder(bigram, n), y = n)) +
  geom_col(aes(fill = bigram)) +
  xlab(NULL) +
  guides(fill = FALSE) +
  coord_flip()

# ad_jobtitle_indeed #

