// Task 1i

db.keywords.aggregate([
    { $match: { keywords: { $elemMatch: { $or: [{ name: "mickey mouse" }, { name: "marvel comic" } ]}}}},
    { $sort: { movieId: 1 } },
    { $project: { movieId: 1, _id: 0 } }

]);