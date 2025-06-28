import scrapy
import re
from datetime import datetime
from urllib.parse import urljoin

class PremierLeagueSpider(scrapy.Spider):
    name = "pl_matches"
    allowed_domains = ['worldfootball.net']
    
    # Multiple starting URLs for comprehensive data
    start_urls = [
        'https://www.worldfootball.net/competition/eng-premier-league/',  # Main competition page
        'https://www.worldfootball.net/all_matches/eng-premier-league-2022-2023/',  # All matches
        'https://www.worldfootball.net/alltime_table/eng-premier-league/',  # League table
    ]
    
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'DOWNLOAD_DELAY': 2,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'CONCURRENT_REQUESTS': 1,
        'FEEDS': {
            'premier_league_complete.json': {'format': 'json'},
            'premier_league_complete.csv': {'format': 'csv'},
        },
        'ROBOTSTXT_OBEY': False,
    }

    def parse(self, response):
        """Main parser that routes to specific parsers based on URL"""
        url = response.url
        
        if 'all_matches' in url:
            yield from self.parse_matches(response)
        elif 'table' in url:
            yield from self.parse_league_table(response)
        elif 'schedule' in url:
            yield from self.parse_schedule(response)
        else:
            # Parse main competition page and find additional links
            yield from self.parse_competition_page(response)

    def parse_matches(self, response):
        """Parse all matches data"""
        self.logger.info(f"Parsing matches from: {response.url}")
        
        # Find all tables
        tables = response.css('table.standard_tabelle, table')
        self.logger.info(f"Found {len(tables)} tables")
        
        match_count = 0
        
        for table in tables:
            rows = table.css('tr')
            
            for row in rows:
                cells = row.css('td')
                
                if len(cells) >= 5:  # Minimum cells for match data
                    # Extract all possible data from cells
                    cell_data = []
                    for i, cell in enumerate(cells):
                        text = cell.css('::text').getall()
                        links = cell.css('a::attr(href)').getall()
                        cell_data.append({
                            'text': ' '.join(text).strip(),
                            'links': links,
                            'position': i
                        })
                    
                    # Try to identify match data pattern
                    if len(cell_data) >= 5:
                        date = cell_data[0]['text']
                        time = cell_data[1]['text'] if len(cell_data) > 1 else ''
                        home_team = cell_data[2]['text']
                        score = cell_data[3]['text'] if len(cell_data) > 3 else ''
                        away_team = cell_data[4]['text'] if len(cell_data) > 4 else ''
                        
                        # Additional data fields
                        venue = cell_data[5]['text'] if len(cell_data) > 5 else ''
                        attendance = cell_data[6]['text'] if len(cell_data) > 6 else ''
                        referee = cell_data[7]['text'] if len(cell_data) > 7 else ''
                        
                        # Validate match data
                        if (date and home_team and away_team and
                            len(home_team) > 1 and len(away_team) > 1 and
                            not any(word in home_team.lower() for word in ['date', 'time', 'team', 'venue'])):
                            
                            match_count += 1
                            
                            # Extract match details link for more data
                            match_link = None
                            for cell in cells:
                                link = cell.css('a::attr(href)').get()
                                if link and 'report' in link:
                                    match_link = urljoin(response.url, link)
                                    break
                            
                            match_data = {
                                'data_type': 'match',
                                'match_id': f"{date}_{home_team}_{away_team}".replace(' ', '_').replace('/', '_'),
                                'date': date,
                                'time': time,
                                'home_team': home_team,
                                'away_team': away_team,
                                'score': score,
                                'venue': venue,
                                'attendance': attendance,
                                'referee': referee,
                                'match_link': match_link,
                                'season': '2022-2023',
                                'league': 'Premier League',
                                'scraped_at': datetime.now().isoformat(),
                                'source_url': response.url,
                            }
                            
                            yield match_data
                            
                            # Follow match link for detailed data
                            if match_link:
                                yield response.follow(match_link, self.parse_match_details, 
                                                    meta={'match_data': match_data})
        
        self.logger.info(f"Scraped {match_count} matches")
        
        # Look for pagination
        pagination_links = response.css('a[title*="page"]::attr(href)').getall()
        for link in pagination_links:
            if link:
                yield response.follow(link, self.parse_matches)

    def parse_match_details(self, response):
        """Parse detailed match information"""
        match_data = response.meta['match_data']
        
        # Extract lineups, statistics, etc.
        lineups = {}
        statistics = {}
        
        # Home team lineup
        home_lineup = response.css('.aufstellung_heim .standard_tabelle tr')
        lineups['home'] = []
        for player_row in home_lineup:
            player_cells = player_row.css('td')
            if len(player_cells) >= 2:
                lineups['home'].append({
                    'number': player_cells[0].css('::text').get(),
                    'name': player_cells[1].css('::text').get(),
                    'position': player_cells[2].css('::text').get() if len(player_cells) > 2 else ''
                })
        
        # Away team lineup
        away_lineup = response.css('.aufstellung_gast .standard_tabelle tr')
        lineups['away'] = []
        for player_row in away_lineup:
            player_cells = player_row.css('td')
            if len(player_cells) >= 2:
                lineups['away'].append({
                    'number': player_cells[0].css('::text').get(),
                    'name': player_cells[1].css('::text').get(),
                    'position': player_cells[2].css('::text').get() if len(player_cells) > 2 else ''
                })
        
        # Match statistics
        stats_rows = response.css('.standard_tabelle tr')
        for row in stats_rows:
            cells = row.css('td')
            if len(cells) == 3:
                stat_name = cells[1].css('::text').get()
                home_stat = cells[0].css('::text').get()
                away_stat = cells[2].css('::text').get()
                if stat_name:
                    statistics[stat_name.strip()] = {
                        'home': home_stat.strip() if home_stat else '',
                        'away': away_stat.strip() if away_stat else ''
                    }
        
        # Match events (goals, cards, etc.)
        events = []
        event_rows = response.css('.ereignisse tr')
        for row in event_rows:
            cells = row.css('td')
            if len(cells) >= 3:
                events.append({
                    'minute': cells[0].css('::text').get(),
                    'event': cells[1].css('::text').get(),
                    'player': cells[2].css('::text').get(),
                })
        
        yield {
            'data_type': 'match_details',
            'match_id': match_data['match_id'],
            'lineups': lineups,
            'statistics': statistics,
            'events': events,
            'scraped_at': datetime.now().isoformat(),
        }

    def parse_league_table(self, response):
        """Parse league table/standings"""
        self.logger.info(f"Parsing league table from: {response.url}")
        
        table_rows = response.css('table.standard_tabelle tr')
        
        for row in table_rows:
            cells = row.css('td')
            
            if len(cells) >= 8:  # Standard league table columns
                position = cells[0].css('::text').get()
                team_name = cells[1].css('a::text').get() or cells[1].css('::text').get()
                games_played = cells[2].css('::text').get()
                wins = cells[3].css('::text').get()
                draws = cells[4].css('::text').get()
                losses = cells[5].css('::text').get()
                goals_for = cells[6].css('::text').get()
                goals_against = cells[7].css('::text').get()
                goal_diff = cells[8].css('::text').get() if len(cells) > 8 else ''
                points = cells[9].css('::text').get() if len(cells) > 9 else ''
                
                if position and team_name and position.isdigit():
                    yield {
                        'data_type': 'league_table',
                        'position': int(position),
                        'team': team_name.strip(),
                        'games_played': int(games_played) if games_played and games_played.isdigit() else 0,
                        'wins': int(wins) if wins and wins.isdigit() else 0,
                        'draws': int(draws) if draws and draws.isdigit() else 0,
                        'losses': int(losses) if losses and losses.isdigit() else 0,
                        'goals_for': int(goals_for) if goals_for and goals_for.isdigit() else 0,
                        'goals_against': int(goals_against) if goals_against and goals_against.isdigit() else 0,
                        'goal_difference': goal_diff,
                        'points': int(points) if points and points.isdigit() else 0,
                        'season': '2022-2023',
                        'scraped_at': datetime.now().isoformat(),
                    }


    def parse_competition_page(self, response):
        """Parse main competition page for additional links"""
        self.logger.info(f"Parsing competition page: {response.url}")
        
        # Find additional useful links
        additional_links = [
            'https://www.worldfootball.net/players/eng-premier-league-2022-2023/Players',  # Players
            'https://www.worldfootball.net/scorer/eng-premier-league-2022-2023/',  # Top scorers
            'https://www.worldfootball.net/assists/eng-premier-league-2022-2023/',  # Assists
        ]
        
        for link in additional_links:
            yield response.follow(link, self.parse_additional_data)

    def parse_additional_data(self, response):
        """Parse additional data like top scorers, assists, etc."""
        url = response.url
        
        if 'scorer' in url:
            yield from self.parse_top_scorers(response)
        elif 'assists' in url:
            yield from self.parse_assists(response)
        elif 'attendance' in url:
            yield from self.parse_attendance(response)
        elif 'players' in url:
            yield from self.parse_players(response)

    def parse_top_scorers(self, response):
        """Parse top scorers data"""
        table_rows = response.css('table.standard_tabelle tr')
        
        for row in table_rows:
            cells = row.css('td')
            if len(cells) >= 4:
                rank = cells[0].css('::text').get()
                player_name = cells[1].css('a::text').get() or cells[1].css('::text').get()
                team = cells[2].css('::text').get()
                goals = cells[3].css('::text').get()
                
                if rank and player_name and rank.isdigit():
                    yield {
                        'data_type': 'top_scorer',
                        'rank': int(rank),
                        'player': player_name.strip(),
                        'team': team.strip() if team else '',
                        'goals': int(goals) if goals and goals.isdigit() else 0,
                        'season': '2022-2023',
                        'scraped_at': datetime.now().isoformat(),
                    }

    def parse_assists(self, response):
        """Parse assists data"""
        table_rows = response.css('table.standard_tabelle tr')
        
        for row in table_rows:
            cells = row.css('td')
            if len(cells) >= 4:
                rank = cells[0].css('::text').get()
                player_name = cells[1].css('a::text').get() or cells[1].css('::text').get()
                team = cells[2].css('::text').get()
                assists = cells[3].css('::text').get()
                
                if rank and player_name and rank.isdigit():
                    yield {
                        'data_type': 'assists',
                        'rank': int(rank),
                        'player': player_name.strip(),
                        'team': team.strip() if team else '',
                        'assists': int(assists) if assists and assists.isdigit() else 0,
                        'season': '2022-2023',
                        'scraped_at': datetime.now().isoformat(),
                    }

    def parse_attendance(self, response):
        """Parse attendance data"""
        table_rows = response.css('table.standard_tabelle tr')
        
        for row in table_rows:
            cells = row.css('td')
            if len(cells) >= 3:
                team = cells[0].css('::text').get()
                avg_attendance = cells[1].css('::text').get()
                total_attendance = cells[2].css('::text').get()
                
                if team and avg_attendance:
                    yield {
                        'data_type': 'attendance',
                        'team': team.strip(),
                        'average_attendance': avg_attendance.strip(),
                        'total_attendance': total_attendance.strip() if total_attendance else '',
                        'season': '2022-2023',
                        'scraped_at': datetime.now().isoformat(),
                    }

    def parse_players(self, response):
        """Parse players data"""
        table_rows = response.css('table.standard_tabelle tr')
        
        for row in table_rows:
            cells = row.css('td')
            if len(cells) >= 4:
                player_name = cells[0].css('a::text').get() or cells[0].css('::text').get()
                position = cells[1].css('::text').get()
                team = cells[2].css('::text').get()
                age = cells[3].css('::text').get()
                
                if player_name and team:
                    yield {
                        'data_type': 'player',
                        'name': player_name.strip(),
                        'position': position.strip() if position else '',
                        'team': team.strip(),
                        'age': age.strip() if age else '',
                        'season': '2022-2023',
                        'scraped_at': datetime.now().isoformat(),
                    }