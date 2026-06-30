# Improving the Claude experinece for PypeIt's ADAP repository

## Init

The files in this project run the PypeIt software package in a cloud instance.
- the data are stored in AWS.
- kubernetes manages the data processing.
- the cloud provider is called nautilus, and the yaml files that control jobs there are in nautilus_jobs
- data access often uses rclone to move files from one host to another (say from the AWS to the cloud computing.)
- config files contain information for PypeIt as well as rclone
- data are downloaded using a serious of scripts in the download_lib directory
- PypeIt is in ~/src/PypeIt
- PypeIt development, which includes cloud tests, are in ~/src/PypeIt-development-suite/

Generate a CLAUDE.md file to guide adapting the adap suite. This suite is made to run PypeIt in the nautilus cloud. 
- yaml files for the kubernetes instances are in nautilus_jobs
- tests are in kube_tests
- config files for controlling how PypeIt is run are in config
- Python and bash scripts for execution of data processing are in scripts

## Skills

Suggested [Claude Code skills](https://docs.claude.com/en/docs/claude-code/skills)
to add for adap development. **All skills live in the
`adap/` repository** under `.claude/skills/<name>/SKILL.md`,
regardless of which repository the skill operates on. The "lives in" URL is the location in
this repo; the "based on" URL is the existing PypeIt doc/code the skill codifies.


## Prompts

1. Perform the 1st step under Init.
2. Given your understanding of the code base, provide a list of suggested skills to add for Claude.  Provide the list in the Skills section above and include URLs to their locations on GitHub.
3. Modify your Skill suggestions to all be located in the adap/ repository.  Then proceed to generate each of these.
4. Update the CLAUDE.md file to be placed in adap/.  As useful, have it refer to both repositories.
