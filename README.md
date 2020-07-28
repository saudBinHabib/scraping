Scraping Task
==========

This is a repo, which is created for a scraping and data pre-processing task.

Useage
~~~~~~~

::
    git clone git@github.com:saudBinHabib/scraping
    cd scraping
    pip install -r requirements/dev.txt
    
    #check crontab for existing cron tasks
    crontab -l

    # add the new task in the crontab
    crontab -e
    
    # add this code in the bottom of file and change the file path of your python script.
    */15 * * * * cd /mnt/e/github/scraping/src/ && python scraping_script.py


~~~~~~~~~~~~~~~~



