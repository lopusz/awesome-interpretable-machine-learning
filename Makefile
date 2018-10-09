README.org: sbin/README.template.org sbin/gener_readme.py 
	cd sbin; ./gener_readme.py --readme-template README.template.org --cache-fname cache.jsonl > ../README.org 
