# One-time backfill 

## run queries

`python3 get-all-days.py`

retrieves all the filtered log data in 30 min intervals to avoid hitting 1000 limit

## format and process

`zsh do-all.sh | grep -v Event`

runs the munge2.py on all the txt files to preprocess and then runs the lambda pipeline.  the local lambda echos all events which can be huge, so filter out.

note that the `DB_HOST` and `DB_PASSWORD` env vars must be present and you should have 
activated the environment via

`. ./venv/bin/activate`
