.PHONY: site

site:
	uv run python leaderboard/build_site.py
	cd leaderboard/starlight && npm run build
