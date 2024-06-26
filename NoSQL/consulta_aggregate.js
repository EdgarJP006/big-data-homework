db.restaurants.aggregate([
    {
        $match: {
            borough: "Manhattan",
            cuisine: { $in: ["Spanish", "Italian"] },
            "grades.score": { $not: { $lt: 12 } }
        }
    },
    {
        $group: {
            _id: { street: "$address.street", cuisine: "$cuisine" },
            restaurants: { $push: { name: "$name", address: "$address" } },
            count: { $sum: 1 }
        }
    },
    {
        $unwind: "$restaurants"
    },
    {
        $sort: { "restaurants.name": 1 }
    },
    {
        $group: {
            _id: "$_id",
            restaurants: { $push: "$restaurants" },
            count: { $first: "$count" }
        }
    },
    {
        $addFields: {
            restaurants: { $slice: ["$restaurants", 2] }
        }
    },
    {
        $project: {
            _id: 0,
            street: '$_id.street',
            cuisine: '$_id.cuisine',
            restaurants: 1,
            count: 1
        }
    },
    {
        $out: "rest_aggregate"
    }
])