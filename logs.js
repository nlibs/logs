const SQLITE = require("sqlite");
const SCHEMA = {"data": {"columns": [["k","INTEGER"], "v"], "index": [], "primary": "k" }}

// writes to disk every 10 mins
const WRITE_FREQUENCY = 60 * 1000; 

// writes early to disk when buffer reaches 256 kb
const BUCKET_LIMIT = 1024 * 256;

class Log
{
	constructor(path)
	{
		var _this = this;
		this.current_size = 0;
		this.current_logs = [];
		this.db = new SQLITE(path, SCHEMA);
		setInterval(_this.write, WRITE_FREQUENCY);
	}

	log(msg)
	{
		var ts = (Date.now() / 1000) | 1;
		this.current_size += msg.length;
		this.current_logs.push([ts, msg]);

		if (this.current_size > BUCKET_LIMIT)
			this.write();
	}

	write()
	{
		var ts = (Date.now() / 1000) | 1;
		var params = [ts, JSON.stringify(this.current_logs)];
		this.db.write("REPLACE INTO data (k,v) VALUES (?,?)", params);
	}

	read(start, end)
	{
		var params = [start, end];
		return this.db.read("SELECT v FROM data WHERE k >= ? AND k < ?", params);
	}
}
