const SQLITE = require("sqlite");
const SCHEMA = {"data": {"columns": [["k","INTEGER"], "v"], "index": [], "primary": "k" }}

// writes to disk every 10 mins
const WRITE_FREQUENCY = 60 * 1000; 

// writes early to disk when buffer reaches 256 kb
const BUCKET_LIMIT = 1024 * 256;

class Logs
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
		var result = this.db.read("SELECT v FROM data WHERE k >= ? AND k < ?", params);
		var r = [];

		for (var i=0;i<result.length;i++)
		{
			var v = JSON.parse(result[i]["v"]);
			for (var j=0;j<v.length;j++)
				r.push(v[j]);
		}
		return r;
	}

	bind(H, url)
	{
		var _this = this;
		H.get(url, function()
		{
			var mandatory = {"start": "int", "end": "int"}
			q = H.parse_fields(q, res, mandatory, {})
			if (!q)
				return;

			var r = _this.read(q.start, q.end)
			H.end(res, 200, JSON.stringify(r));
		})
	}
}
module.exports = Logs;
