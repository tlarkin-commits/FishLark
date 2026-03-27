# FishLark - SoCal & Baja Offshore Fishing Intelligence Platform

## Overview

FishLark is a data-driven fishing intelligence platform for Southern California and Northern Baja offshore fishing. It combines 17 years of daily fish reports from Fishdope.com with bathymetry, NOAA weather/current data, moon phases, and chlorophyll satellite imagery.

## Quick Start

```bash
git clone https://github.com/tlarkin-commits/FishLark.git
cd FishLark/fishdope_pipeline
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python scraper.py --start-page 1 --end-page 10
python pipeline.py --parse
```

## Project Structure

- fishdope_pipeline/config.py - Configuration, zone definitions, species mapping
- fishdope_pipeline/schema.py - Normalized JSON data schema
- fishdope_pipeline/scraper.py - Scrape raw HTML from all Fishdope pages
- fishdope_pipeline/parser_rules.py - Regex-based field extraction
- fishdope_pipeline/parser_llm.py - LLM-based extraction for complex prose
- fishdope_pipeline/pipeline.py - Orchestrator script

## Data: 17 Years of SoCal Fish Reports

~6,000+ daily reports covering 15+ zones from Santa Barbara to Northern Baja, tracking 20+ species with GPS coordinates, catch quality, conditions, bait, and technique data.
