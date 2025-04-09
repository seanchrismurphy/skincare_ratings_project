@echo off
echo Running Reddit Scraper Debug Version
echo ====================================

echo Checking environment variables...
echo REDDIT_CLIENT_ID: %REDDIT_CLIENT_ID%
echo REDDIT_CLIENT_SECRET: %REDDIT_CLIENT_SECRET%
echo REDDIT_USER_AGENT: %REDDIT_USER_AGENT%

echo.
echo Running debug scraper...
python src/ingestion/reddit_scraper_debug.py

echo.
echo If you see empty DataFrames, check the following:
echo 1. Are your Reddit API credentials set correctly?
echo 2. Is the config.py file in the project root?
echo 3. Are there any error messages in the output?
echo.
echo Press any key to exit...
pause > nul 