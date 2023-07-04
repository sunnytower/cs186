// Task 2i

db.movies_metadata.aggregate([
    { $match: { vote_count: { $gt: 1838 } } },
    {
        $project: {
            title: "$title",
            vote_count: "$vote_count",
            score: { $round: [{ $add: [{ $multiply: ["$vote_average", { $divide: ["$vote_count", { $add: ["$vote_count", 1838] }] }] }, { $multiply: [7, { $divide: [1838, { $add: [1838, "$vote_count"] }] }] }] }, 2] },
            _id: 0,

    }},
    {$sort: {score: -1, vote_count: -1, title: 1}},
    {$limit: 20}
]);