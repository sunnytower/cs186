// Task 1iii

db.ratings.aggregate([
    {
        $group: {
            _id: "$rating",
            count: { $sum: 1 }
        }
    },
    {
        $project: {
            count: 1,
            rating: "$_id",
            _id: 0
        }
    },
    { $sort: { rating: -1 } }
]);