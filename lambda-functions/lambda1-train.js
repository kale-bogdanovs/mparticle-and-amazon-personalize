// Import Dependencies
const AWS = require('aws-sdk');
const JSONBig = require('json-bigint')({
    storeAsString: true
}); // needed to parse 64-bit integer MPID

// Define the product actions we want to report to Personalize
const report_actions = ["purchase", "view_detail", "add_to_cart", "add_to_wishlist"];

// Initialize Personalize
const personalizeevents = new AWS.PersonalizeEvents({
    apiVersion: '2018-03-22'
});

exports.handler = (event, context) => {
    for (const record of event.Records) {
        // Parse encoded payload
        const payload = JSONBig.parse(Buffer.from(record.kinesis.data, 'base64').toString('ascii'));

        // Extract required params
        const events = payload.events;
        const mpid = payload.mpid;
        const sessionId = payload.message_id;
        const params = {
            sessionId: sessionId,
            userId: mpid,
            trackingId: process.env.TRACKING_ID
        };

        // Get interactions from events array
        const eventList = [];
        for (const e of events) {
            if (e.event_type === "commerce_event" && report_actions.indexOf(e.data.product_action.action) >= 0) {
                const timestamp = Math.floor(e.data.timestamp_unixtime_ms / 1000);
                const action = e.data.product_action.action;
                const event_id = e.data.event_id;
                for (const product of e.data.product_action.products) {
                    const obj = {
                        itemId: product.id,
                    };
                    eventList.push({
                        properties: obj,
                        sentAt: timestamp,
                        eventId: event_id,
                        eventType: action
                    });
                }
            }
        }
        if (eventList.length > 0) {
            params.eventList = eventList;
            // Upload interactions to tracker
            personalizeevents.putEvents(params, function(err) {
                if (err) console.log(err, err.stack);
                else console.log(`Uploaded ${eventList.length} events`)
            });
        }
    }
};