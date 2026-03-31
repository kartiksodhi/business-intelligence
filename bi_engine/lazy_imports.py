"""
Lazy loaders for heavy dependencies.

Import at module level is avoided for Playwright and LangGraph because:
- They add 200–400ms to cold start
- Not every pipeline run needs them

Usage:
    from lazy_imports import get_playwright, get_langgraph
    playwright = await get_playwright().__aenter__()
"""

from __future__ import annotations


def get_playwright():
    from playwright.async_api import async_playwright
    return async_playwright


def get_langgraph():
    from langgraph.graph import StateGraph
    return StateGraph


def get_bs4():
    from bs4 import BeautifulSoup
    return BeautifulSoup
