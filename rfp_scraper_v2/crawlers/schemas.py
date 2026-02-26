"""
CSS Selector Schemas for known procurement platforms.
These schemas are used by Crawl4AI's JsonCssExtractionStrategy.
"""

# Bonfire Schema (Best Guess)
BONFIRE_SCHEMA = {
    "baseSelector": "div.opportunity-table table tbody tr",
    "fields": [
        {"name": "title", "selector": "td.title-col a", "type": "text"},
        {"name": "link", "selector": "td.title-col a", "type": "attribute", "attribute": "href"},
        {"name": "deadline", "selector": "td.closing-date-col", "type": "text"},
        {"name": "clientName", "selector": "td.organization-col", "type": "text", "default": "Bonfire Agency"},
        {"name": "description", "selector": "td.description-col", "type": "text", "default": ""},
    ]
}

# IonWave Schema (Best Guess)
IONWAVE_SCHEMA = {
    "baseSelector": "table#bidList tbody tr",
    "fields": [
        {"name": "title", "selector": "td.bid-title a", "type": "text"},
        {"name": "link", "selector": "td.bid-title a", "type": "attribute", "attribute": "href"},
        {"name": "deadline", "selector": "td.close-date", "type": "text"},
        {"name": "clientName", "selector": "td.agency-name", "type": "text", "default": "IonWave Agency"},
    ]
}

# PlanetBids Schema (Best Guess)
PLANETBIDS_SCHEMA = {
    "baseSelector": "div#bidResult tr.gridRow",
    "fields": [
        {"name": "title", "selector": "td.project-title", "type": "text"},
        {"name": "link", "selector": "td.project-title a", "type": "attribute", "attribute": "href"},
        {"name": "deadline", "selector": "td.date-due", "type": "text"},
        {"name": "clientName", "selector": "td.agency", "type": "text", "default": "PlanetBids Agency"},
    ]
}

# OpenGov Schema (Best Guess)
OPENGOV_SCHEMA = {
    "baseSelector": "div.project-list-item",
    "fields": [
        {"name": "title", "selector": "h3.project-title", "type": "text"},
        {"name": "link", "selector": "a.project-link", "type": "attribute", "attribute": "href"},
        {"name": "deadline", "selector": "span.due-date", "type": "text"},
        {"name": "clientName", "selector": "div.agency-name", "type": "text", "default": "OpenGov Agency"},
    ]
}

# BidNet Schema (Best Guess)
BIDNET_SCHEMA = {
    "baseSelector": "table.solicitations-table tbody tr",
    "fields": [
        {"name": "title", "selector": "td.solicitation-title a", "type": "text"},
        {"name": "link", "selector": "td.solicitation-title a", "type": "attribute", "attribute": "href"},
        {"name": "deadline", "selector": "td.closing-date", "type": "text"},
        {"name": "clientName", "selector": "td.agency", "type": "text", "default": "BidNet Agency"},
    ]
}
