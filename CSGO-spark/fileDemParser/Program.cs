using System;
using System.Reflection;
using System.IO;
using DemoInfo;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace StatisticsGenerator
{
	class MainClass
	{
		public static void Main (string[] args)
		{
			// First, check wether the user needs assistance:
			if (args.Length == 0 || args [0] == "--help") {
				PrintHelp ();
				return;
			}

			// Every argument is a file, so let's iterate over all the arguments
			// So you can call this program like
			// > StatisticsGenerator.exe hello.dem bye.dem
			// It'll generate the statistics.
			foreach (var fileName in args) {
				// Okay, first we need to initalize a demo-parser
				// It takes a stream, so we simply open with a filestream
                int matchStartEventNumber = 0;

				using (var fileStream = File.OpenRead (fileName)) {
					// By using "using" we make sure that the fileStream is properly disposed
					// the same goes for the DemoParser which NEEDS to be disposed (else it'll
					// leak memory and kittens will die. 

					Console.WriteLine ("Parsing demo " + fileName);
                    matchStartEventNumber = findMatchStartRound(fileStream);
				}

                using (var fileStream = File.OpenRead(fileName))
                {
                    parseDemo(fileStream, fileName, matchStartEventNumber);
                }
			}
		
		}

        private static int findMatchStartRound(FileStream fileStream)
        {
            int result = 0;
            using (var parser = new DemoParser(fileStream))
            {
                parser.ParseHeader();
                int roundNumber = 0;

                parser.MatchStarted += (sender, e) =>
                {
                    if (roundNumber <= 16)
                    {
                        roundNumber = 0;
                        result++;
                    }
                };

                parser.RoundEnd += (object sender, RoundEndedEventArgs e) =>
                {
                    roundNumber++;
                };

                parser.ParseToEnd();
            }

            Console.WriteLine("Match starts at round number " + result);
            return result;
        }

        static void parseDemo(FileStream fileStream, string fileName, int matchStartEventNumber)
        {
            using (var parser = new DemoParser(fileStream))
            {
                string demoName = Path.GetFileName(fileName);
                // So now we've initialized a demo-parser. 
                // let's parse the head of the demo-file to get which map the match is on!
                // this is always the first step you need to do.
                parser.ParseHeader();

                // and now, do some magic: grab the match!
                string map = parser.Map;

                // And now, generate the filename of the resulting file
                string output_roundResults = fileName + "_roundResults.csv";
                string output_killEvents = fileName + "_killEvents.csv";
                string output_matchResults = fileName + "_matchResults.csv";
                /*string output_smokeNades = fileName + "_smokeNades.csv";
                string output_fireNades = fileName + "_fireNades.csv";
                string output_flashNades = fileName + "_flashNades.csv";
                string output_decoyNades = fileName + "_decoyNades.csv";
                string output_heNades = fileName + "_heNades.csv";*/
                // and open it. 
                var outputStream_roundResults = new StreamWriter(output_roundResults, false, Encoding.UTF8);
                var outputStream_killEvents = new StreamWriter(output_killEvents, false, Encoding.UTF8);
                var outputStream_matchResults = new StreamWriter(output_matchResults, false, Encoding.UTF8);
                /*var outputStream_smokeNades = new StreamWriter(output_smokeNades, false, Encoding.UTF8);
                var outputStream_fireNades = new StreamWriter(output_fireNades, false, Encoding.UTF8);
                var outputStream_flashNades = new StreamWriter(output_flashNades, false, Encoding.UTF8);
                var outputStream_decoyNades = new StreamWriter(output_decoyNades, false, Encoding.UTF8);
                var outputStream_heNades = new StreamWriter(output_heNades, false, Encoding.UTF8);*/

                //And write a header so you know what is what in the resulting file
                //outputStream.WriteLine(GenerateCSVHeader());

                int ctStartroundMoney = 0, tStartroundMoney = 0, ctEquipValue = 0, tEquipValue = 0, ctSaveAmount = 0, tSaveAmount = 0;
                int numberOfEventsMatchStarted = 0;
                int roundNumber = 0;

                List<String> ctWeapons = new List<String>();
                List<String> tWeapons = new List<String>();

                float ctWay = 0, tWay = 0;
                
                bool teamsSwap = false;

                int CTScore = 0;
                int TScore = 0;

                int defuses = 0;
                int plants = 0;

                string firstRoundWinner = "";
                string lastRoundWinner = "";

                Dictionary<Player, int> killsThisRound = new Dictionary<Player, int>();

                // Since most of the parsing is done via "Events" in CS:GO, we need to use them. 
                // So you bind to events in C# as well. 

                // AFTER we have bound the events, we start the parser!


                parser.MatchStarted += (sender, e) =>
                {
                    numberOfEventsMatchStarted++;
                };

                parser.PlayerKilled += (object sender, PlayerKilledEventArgs e) =>
                {

                    if (numberOfEventsMatchStarted!=matchStartEventNumber)
                        return;

                    //the killer is null if you're killed by the world - eg. by falling
                    if (e.Killer != null)
                    {
                        if (!killsThisRound.ContainsKey(e.Killer))
                            killsThisRound[e.Killer] = 0;

                        //Remember how many kills each player made this rounds
                        try
                        {
                            killsThisRound[e.Killer]++;
                            string killerSteamID = e.Killer.SteamID.ToString();
                            Vector killerPosition = e.Killer.Position;
                            string killingWeapon = e.Weapon.Weapon.ToString();
                            bool isHeadshot = e.Headshot;
                            int penetratedObjects = e.PenetratedObjects;
                            string victimSteamID = e.Victim.SteamID.ToString();
                            Vector victimPosition = e.Victim.Position;
                            PrintKillEvent(parser, outputStream_killEvents, demoName, map, killerSteamID, killerPosition, killingWeapon, isHeadshot, penetratedObjects, victimSteamID, victimPosition);
                        }
                        catch (Exception)
                        {
                            return;
                        }
                    }
                };

                parser.RoundStart += (sender, e) =>
                {
                    if (numberOfEventsMatchStarted != matchStartEventNumber)
                        return;
                    //How much money had each team at the start of the round?
                    ctStartroundMoney = parser.Participants.Where(a => a.Team == Team.CounterTerrorist).Sum(a => a.Money);
                    tStartroundMoney = parser.Participants.Where(a => a.Team == Team.Terrorist).Sum(a => a.Money);

                    //And how much they did they save from the last round?
                    ctSaveAmount = parser.Participants.Where(a => a.Team == Team.CounterTerrorist && a.IsAlive).Sum(a => a.CurrentEquipmentValue);
                    tSaveAmount = parser.Participants.Where(a => a.Team == Team.Terrorist && a.IsAlive).Sum(a => a.CurrentEquipmentValue);

                    //And let's reset those statistics
                    ctWay = 0; tWay = 0;
                    plants = 0; defuses = 0;
                    ctWeapons = new List<String>();
                    tWeapons = new List<String>();

                    killsThisRound.Clear();
                };

                parser.FreezetimeEnded += (sender, e) =>
                {
                    if (numberOfEventsMatchStarted != matchStartEventNumber)
                        return;

                    // At the end of the freezetime (when players can start walking)
                    // calculate the equipment value of each team!
                    ctEquipValue = parser.Participants.Where(a => a.Team == Team.CounterTerrorist).Sum(a => a.CurrentEquipmentValue);
                    List<Player> ctPLayers = parser.Participants.Where(a => a.Team == Team.CounterTerrorist).ToList();
                    tEquipValue = parser.Participants.Where(a => a.Team == Team.Terrorist).Sum(a => a.CurrentEquipmentValue);
                    List<Player> tPLayers = parser.Participants.Where(a => a.Team == Team.Terrorist).ToList();

                    foreach (Player p in ctPLayers)
                    {
                        foreach (Equipment e2 in p.Weapons)
                            ctWeapons.Add(e2.Weapon.ToString());
                        ctWeapons.Add("END");
                    }
                    foreach (Player p in tPLayers)
                    {
                        foreach (Equipment e2 in p.Weapons)
                            tWeapons.Add(e2.Weapon.ToString());
                        tWeapons.Add("END");
                    }
                };

                parser.BombPlanted += (sender, e) =>
                {
                    if (numberOfEventsMatchStarted != matchStartEventNumber)
                        return;

                    plants++;
                };

                /*parser.SmokeNadeStarted += (sender, e) =>
                {
                    if (numberOfEventsMatchStarted != matchStartEventNumber)
                        return;

                    Vector nadePosition = e.Position;
                    PrintSmokeEvent(parser, outputStream_smokeNades, demoName, map, nadePosition);

                };

                parser.FireNadeStarted += (sender, e) =>
                {
                    if (numberOfEventsMatchStarted != matchStartEventNumber)
                        return;

                    Vector nadePosition = e.Position;
                    PrintFireNadeEvent(parser, outputStream_fireNades, demoName, map, nadePosition);

                };

                parser.FlashNadeExploded += (sender, e) =>
                {
                    if (numberOfEventsMatchStarted != matchStartEventNumber)
                        return;

                    Vector nadePosition = e.Position;
                    PrintFlashNadeEvent(parser, outputStream_flashNades, demoName, map, nadePosition);

                };

                parser.DecoyNadeStarted += (sender, e) =>
                {
                    if (numberOfEventsMatchStarted != matchStartEventNumber)
                        return;

                    Vector nadePosition = e.Position;
                    PrintDecoyNadeEvent(parser, outputStream_decoyNades, demoName, map, nadePosition);

                };

                parser.ExplosiveNadeExploded += (sender, e) =>
                {
                    if (numberOfEventsMatchStarted != matchStartEventNumber)
                        return;

                    Vector nadePosition = e.Position;
                    PrintHeNadeExplosionEvent(parser, outputStream_heNades, demoName, map, nadePosition);

                };*/

                parser.BombDefused += (sender, e) =>
                {
                    if (numberOfEventsMatchStarted != matchStartEventNumber)
                        return;

                    defuses++;
                };

                parser.LastRoundHalf += (sender, e) =>
                {
                    if (numberOfEventsMatchStarted != matchStartEventNumber)
                        return;

                    teamsSwap = true;
                    
                };

                parser.TickDone += (sender, e) =>
                {
                    if (numberOfEventsMatchStarted != matchStartEventNumber)
                        return;

                    // Okay, let's measure how far each team travelled. 
                    // As you might know from school the amount walked
                    // by a player is the sum of it's velocities

                    foreach (var player in parser.PlayingParticipants)
                    {
                        // We multiply it by the time of one tick
                        // Since the velocity is given in 
                        // ingame-units per second
                        float currentWay = (float)(player.Velocity.Absolute * parser.TickTime);

                        // This is just an example of what kind of stuff you can do
                        // with this parser. 
                        // Of course you could find out who makes the most footsteps, and find out
                        // which player ninjas the most - just to give you an example

                        if (player.Team == Team.CounterTerrorist)
                            ctWay += currentWay;
                        else if (player.Team == Team.Terrorist)
                            tWay += currentWay;
                    }
                };

                //So now lets do some fancy output
                parser.RoundEnd += (object sender, RoundEndedEventArgs e) =>
                {
                    if (numberOfEventsMatchStarted != matchStartEventNumber)
                    {
                        return;
                    }
                    
                    roundNumber++;

                    // We do this in a method-call since we'd else need to duplicate code
                    // The much parameters are there because I simply extracted a method
                    // Sorry for this - you should be able to read it anywys :)
                    string winningFaction = e.Winner.ToString();

                    if (roundNumber == 1)
                    {
                        if (winningFaction.Equals("CounterTerrorist"))
                        {
                            firstRoundWinner = parser.CTClanName;
                        }
                        if (winningFaction.Equals("Terrorist"))
                        {
                            firstRoundWinner = parser.TClanName;
                        }
                    }

                    if (winningFaction.Equals("CounterTerrorist"))
                    {
                        CTScore++;
                        lastRoundWinner = parser.CTClanName;
                    }
                    if (winningFaction.Equals("Terrorist"))
                    {
                        lastRoundWinner = parser.TClanName;
                        TScore++;
                    }

                    PrintRoundResults(parser, outputStream_roundResults, demoName, map, CTScore, TScore, ctStartroundMoney, tStartroundMoney, ctEquipValue, tEquipValue, ctSaveAmount, tSaveAmount, ctWay, tWay, defuses, plants, killsThisRound, winningFaction, ctWeapons, tWeapons);

                    if (teamsSwap)
                    {
                        teamsSwap = false;
                        int tempCTscore = CTScore;
                        int tempTscore = TScore;
                        CTScore = tempTscore;
                        TScore = tempCTscore;
                    }

                };

                //Now let's parse the demo!
                parser.ParseToEnd();

                PrintMatchResults(parser, outputStream_matchResults, demoName, map, parser.CurrentTime, parser.Header.ClientName, parser.Header.NetworkProtocol, parser.Header.Protocol, parser.Header.SignonLength, parser.Header.PlaybackTime, firstRoundWinner, lastRoundWinner);
                
                outputStream_roundResults.Close();
                outputStream_killEvents.Close();
                outputStream_matchResults.Close();
               /* outputStream_smokeNades.Close();
                outputStream_fireNades.Close();
                outputStream_flashNades.Close();
                outputStream_decoyNades.Close();
                outputStream_heNades.Close();*/
            }
        }

		static string GenerateCSVHeader()
		{
			return string.Format(
                "{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16},{17},{18},{19},{20},{21},{22},{23},{24},{25},{26},{27},{28}", // number of columns on the csv file to write the titles
				"File-Name",
                "Event-Type",
                "NapName",
                "Round-Number|Killer-Name", // parser.CTScore + parser.TScore, //Round-Number
				"CT-Score|Killer-Position", // parser.CTScore,
				"T-Score|Killer-Weapon", // parser.TScore,
				//how many CTs are still alive?
				"SurvivingCTs|Headshot", // parser.PlayingParticipants.Count(a => a.IsAlive && a.Team == Team.CounterTerrorist),
				//how many Ts are still alive?
				"SurvivingTs|PenetratedObjects", // parser.PlayingParticipants.Count(a => a.IsAlive && a.Team == Team.Terrorist),
				"CT-StartMoney|Killed-Name", // ctStartroundMoney,
				"T-StartMoney|Killed-Position", // tStartroundMoney,
				"CT-EquipValue", // ctEquipValue,
				"T-EquipValue", // tEquipValue,
				"CT-SavedFromLastRound", // ctSaveAmount,
				"T-SavedFromLastRound", // tSaveAmount,
				"WalkedCTWay", // ctWay,
				"WalkedTWay", // tWay,
				//The kills of all CTs so far
				"CT-Kills", // parser.PlayingParticipants.Where(a => a.Team == Team.CounterTerrorist).Sum(a => a.AdditionaInformations.Kills),
				"T-Kills", // parser.PlayingParticipants.Where(a => a.Team == Team.Terrorist).Sum(a => a.AdditionaInformations.Kills),
				//The deaths of all CTs so far
				"CT-Deaths", // parser.PlayingParticipants.Where(a => a.Team == Team.CounterTerrorist).Sum(a => a.AdditionaInformations.Deaths),
				"T-Deaths", // parser.PlayingParticipants.Where(a => a.Team == Team.Terrorist).Sum(a => a.AdditionaInformations.Deaths),
				//The assists of all CTs so far
				"CT-Assists", // parser.PlayingParticipants.Where(a => a.Team == Team.CounterTerrorist).Sum(a => a.AdditionaInformations.Assists),
				"T-Assists", // parser.PlayingParticipants.Where(a => a.Team == Team.Terrorist).Sum(a => a.AdditionaInformations.Assists),
				"BombPlanted", // plants,
				"BombDefused", // defuses,
				"TopfraggerSteamid", // topfragger.Key.SteamID, //The steamid of the topfragger this round
				"TopfraggerKillsThisRound", // topfragger.Value //The amount of kills he got
                "WinningFaction", // topfragger.Value //The amount of kills he got
                "CT Equipment",
                "T Equipment"
              
			);
		}

		static void PrintHelp ()
		{
			string fileName = Path.GetFileName((Assembly.GetExecutingAssembly().Location));
			Console.WriteLine ("CS:GO Demo-Statistics-Generator");
			Console.WriteLine ("http://github.com/moritzuehling/demostatistics-creator");
			Console.WriteLine ("------------------------------------------------------");
			Console.WriteLine ("Usage: {0} [--help] [--scoreboard] file1.dem [file2.dem ...]", fileName);
			Console.WriteLine ("--help");
			Console.WriteLine ("    Displays this help");
			Console.WriteLine ("--frags");
			Console.WriteLine ("    Displays only the frags happening in this demo in the format. Cannot be used with anything else. Only works with 1 file. ");
			Console.WriteLine ("    Output-format (example): <Hyper><76561198014874496><CT> + <centralize><76561198085059888><CT> [M4A1][HS][Wall] <percy><76561197996475850><T>");
			Console.WriteLine ("--scoreboard");
			Console.WriteLine ("    Displays only the scoreboards on every round_end event. Cannot be used with anything else. Only works with 1 file. ");

			Console.WriteLine ("file1.dem");
			Console.WriteLine ("    Path to a demo to be parsed. The resulting file with have the same name, ");
			Console.WriteLine ("    except that it'll end with \".dem.[map].csv\", where [map] is the map.");
			Console.WriteLine ("    The resulting file will be a CSV-File containing some statistics generated");
			Console.WriteLine ("    by this program, and can be viewed with (for example) LibreOffice");
			Console.WriteLine ("[file2.dem ...]");
			Console.WriteLine ("    You can specify more than one file at a time.");

		}

        static void PrintRoundResults(DemoParser parser, StreamWriter outputStream, string demoName, string mapName, int ctScore, int tScore, int ctStartroundMoney, int tStartroundMoney, int ctEquipValue, int tEquipValue, int ctSaveAmount, int tSaveAmount, float ctWay, float tWay, int defuses, int plants, Dictionary<Player, int> killsThisRound, string winningFaction, List<String> ctEquipment, List<String> tEquipment)
		{
			//okay, get the topfragger:
            string nameEvent = "roundResults";
            var ctWeapons = String.Join("-", ctEquipment.ToArray());
            var tWeapons = String.Join("-", tEquipment.ToArray());
			var topfragger = killsThisRound.OrderByDescending (x => x.Value).FirstOrDefault ();
			if (topfragger.Equals (default(KeyValuePair<Player, int>)))
				topfragger = new KeyValuePair<Player, int> (new Player (), 0);
			//At the end of each round, let's write down some statistics!
            outputStream.WriteLine(string.Format("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16},{17},{18},{19},{20},{21},{22},{23},{24},{25},{26},{27},{28}",
            demoName,
            nameEvent,
            mapName,
            ctScore + tScore, //Round-Number
            ctScore, tScore, //how many CTs are still alive?
			parser.PlayingParticipants.Count (a => a.IsAlive && a.Team == Team.CounterTerrorist), //how many Ts are still alive?
			parser.PlayingParticipants.Count (a => a.IsAlive && a.Team == Team.Terrorist), ctStartroundMoney, tStartroundMoney, ctEquipValue, tEquipValue, ctSaveAmount, tSaveAmount, ctWay, tWay, //The kills of all CTs so far
			parser.PlayingParticipants.Where (a => a.Team == Team.CounterTerrorist).Sum (a => a.AdditionaInformations.Kills), parser.PlayingParticipants.Where (a => a.Team == Team.Terrorist).Sum (a => a.AdditionaInformations.Kills), //The deaths of all CTs so far
			parser.PlayingParticipants.Where (a => a.Team == Team.CounterTerrorist).Sum (a => a.AdditionaInformations.Deaths), parser.PlayingParticipants.Where (a => a.Team == Team.Terrorist).Sum (a => a.AdditionaInformations.Deaths), //The assists of all CTs so far
			parser.PlayingParticipants.Where (a => a.Team == Team.CounterTerrorist).Sum (a => a.AdditionaInformations.Assists), parser.PlayingParticipants.Where (a => a.Team == Team.Terrorist).Sum (a => a.AdditionaInformations.Assists), plants, defuses,
			topfragger.Key.SteamID, //The steamid of the topfragger this round
			topfragger.Value, //The amount of kills he got
            winningFaction,
            ctWeapons,
            tWeapons
			));
		}

        static void PrintKillEvent(DemoParser parser, StreamWriter outputStream, string demoName, string mapName, string killerName, Vector killerPosition, string killerWeapon, bool isHeadshot, int penetradedObjects, string killedName, Vector killedPosition)
        {
            string nameEvent = "kill";
            outputStream.WriteLine(string.Format("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9}",
            demoName,
            nameEvent,
            mapName,
            killerName,
            "X:" + killerPosition.X + ";Y:" + killerPosition.Y + ";Z:" + killerPosition.Z,
            killerWeapon,
            isHeadshot,
            penetradedObjects,
            killedName,
            "X:" + killedPosition.X + ";Y:" + killedPosition.Y + ";Z:" + killedPosition.Z
            ));
        }

        private static void PrintMatchResults(DemoParser parser, StreamWriter outputStream, string demoName, string mapName, float currentTime, string clientName, int networkProtocol, int protocol, int signonLength, float playbackTime, string whoWonFirstRound, string winner)
        {
            string nameEvent = "matchEnd";
            outputStream.WriteLine(string.Format("{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10}",
            demoName,
            nameEvent,
            mapName,
            currentTime,
            clientName,
            networkProtocol,
            protocol,
            signonLength,
            playbackTime,
            whoWonFirstRound.Replace(" ", string.Empty),
            winner.Replace(" ", string.Empty)
            ));
        }

        /*private static void PrintSmokeEvent(DemoParser parser, StreamWriter outputStream, string demoName, string mapName, Vector position)
        {
            string nameEvent = "smokeNade";
            outputStream.WriteLine(string.Format("{0},{1},{2},{3}",
            demoName,
            nameEvent,
            mapName,
            "X:" + position.X + ";Y:" + position.Y + ";Z:" + position.Z
            ));
        }

        private static void PrintFireNadeEvent(DemoParser parser, StreamWriter outputStream, string demoName, string mapName, Vector position)
        {
            string nameEvent = "fireNade";
            outputStream.WriteLine(string.Format("{0},{1},{2},{3}",
            demoName,
            nameEvent,
            mapName,
            "X:" + position.X + ";Y:" + position.Y + ";Z:" + position.Z
            ));
        }

        private static void PrintFlashNadeEvent(DemoParser parser, StreamWriter outputStream, string demoName, string mapName, Vector position)
        {
            string nameEvent = "flashNade";
            outputStream.WriteLine(string.Format("{0},{1},{2},{3}",
            demoName,
            nameEvent,
            mapName,
            "X:" + position.X + ";Y:" + position.Y + ";Z:" + position.Z
            ));
        }

        private static void PrintDecoyNadeEvent(DemoParser parser, StreamWriter outputStream, string demoName, string mapName, Vector position)
        {
            string nameEvent = "decoyNade";
            outputStream.WriteLine(string.Format("{0},{1},{2},{3}",
            demoName,
            nameEvent,
            mapName,
            "X:" + position.X + ";Y:" + position.Y + ";Z:" + position.Z
            ));
        }

        private static void PrintHeNadeExplosionEvent(DemoParser parser, StreamWriter outputStream, string demoName, string mapName, Vector position)
        {
            string nameEvent = "heNadeExplodedNade";
            outputStream.WriteLine(string.Format("{0},{1},{2},{3}",
            demoName,
            nameEvent,
            mapName,
            "X:" + position.X + ";Y:" + position.Y + ";Z:" + position.Z
            ));
        } */
	}
}
