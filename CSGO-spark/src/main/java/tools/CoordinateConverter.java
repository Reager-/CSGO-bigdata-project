package tools;

public class CoordinateConverter {
	public static void main(String[] args){
		
		// pos_x of the desired map from C:\Program Files (x86)\Steam\SteamApps\common\Counter-Strike Global Offensive\csgo\resource\overviews
		double pos_x = -3230;
		// pos_y of the desired map from C:\Program Files (x86)\Steam\SteamApps\common\Counter-Strike Global Offensive\csgo\resource\overviews
		double pos_y = 1713;
		// scale of the desired map from C:\Program Files (x86)\Steam\SteamApps\common\Counter-Strike Global Offensive\csgo\resource\overviews
		double scale = 5.0;
		
		//Formulas to convert into pixels, insert here value of x and y to convert
		double x = (-689.9311951711571 - pos_x) / scale;
		double y = (-(-469.7565391598533 - pos_y)) / scale;
		System.out.println(x + "," + y);
	}

}
