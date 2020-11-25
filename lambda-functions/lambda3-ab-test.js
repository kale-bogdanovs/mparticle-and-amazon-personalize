const AWS = require('aws-sdk');
const JSONBig = require('json-bigint')({storeAsString: true});
const mParticle = require('mparticle');
const trackingId = "bd973581-6505-46ae-9939-e0642a82b8b4";
const report_actions = ["purchase", "view_detail", "add_to_cart", "add_to_wishlist"];

const personalizeevents = new AWS.PersonalizeEvents({apiVersion: '2018-03-22'});
const personalizeruntime = new AWS.PersonalizeRuntime({apiVersion: '2018-05-22'});
const mp_api = new mParticle.EventsApi(new mParticle.Configuration(process.env.MP_KEY, process.env.MP_SECRET));

exports.handler = function(event, context) {
    for (const record of event.Records) {
        const payload = JSONBig.parse(Buffer.from(record.kinesis.data, 'base64').toString('ascii'));
        const events = payload.events;
        const mpid = payload.mpid;
        const sessionId = payload.message_id;
        const params = {
            sessionId: sessionId,
            userId: mpid,
            trackingId: trackingId
        };
        // Check for variant and assign one if not already assigned
        const variant_assigned = Boolean(payload.user_attributes.ml_variant);
        const variant = variant_assigned ? payload.user_attributes.ml_variant : Math.random() > 0.5 ? "A" : "B";

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
            personalizeevents.putEvents(params, function(err, data) {
                if (err) console.log(err, err.stack);
                else {
                    var params = {
                        // Select campaign based on variant
                        campaignArn: process.env[`CAMPAIGN_ARN_${variant}`],
                        numResults: '5',
                        userId: mpid
                    };
                    personalizeruntime.getRecommendations(params, function(err, data) {
                        if (err) console.log(err, err.stack);
                        else {
                            const batch = new mParticle.Batch(mParticle.Batch.Environment.development);
                            batch.mpid = mpid;
                            const itemList = [];
                            for (const item of data.itemList) {
                                itemList.push(item.itemId);
                            }
                            batch.user_attributes = {};
                            batch.user_attributes.product_recs = itemList;

                            // Record variant on mParticle user profile
                            if (!variant_assigned) {
                                batch.user_attributes.ml_variant = variant
                            }

                            const event = new mParticle.AppEvent(mParticle.AppEvent.CustomEventType.other, 'AWS Recs Update');
                            event.custom_attributes = {
                                product_recs: itemList.join()
                            };
                            batch.addEvent(event);
                            const mp_callback = function(error, data, response) {
                                if (error) {
                                    console.error(error);
                                } else {
                                    console.log('API called successfully.');
                                }
                            };
                            mp_api.uploadEvents(batch, mp_callback);
                        }
                    });
                }
            });
        }
    }
};