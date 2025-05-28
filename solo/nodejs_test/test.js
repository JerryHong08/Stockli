require("dotenv").config();
const { Config, QuoteContext } = require("longport");

let config = Config.fromEnv();

QuoteContext.new(config)
    .then((ctx) => ctx.quote(["700.HK", "AAPL.US", "TSLA.US", "NFLX.US"]))
    .then((resp) => {
        for (let obj of resp) {
            console.log(obj.toString());
        }
    })
    .catch((err) => {
        console.error("出错了:", err);
    });
