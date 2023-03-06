### Data Processing in R and Python 2022Z
### Homework Assignment no. 2
###
### IMPORTANT
### This file should contain only solutions to tasks in the form of a functions
### definitions and comments to the code.
import pandas as pd
import numpy as np

#
# -----------------------------------------------------------------------------#
# Task 1
# -----------------------------------------------------------------------------#


def solution_1(Posts):
    posts2 = Posts
    posts2['CreationDate'] = pd.to_datetime(posts2['CreationDate'])
    posts2['Year'] = posts2['CreationDate'].dt.year
    res = posts2.groupby('Year').size().reset_index(name='TotalNumber')
    res['Year'] = res['Year'].astype('str')
    return res


# -----------------------------------------------------------------------------#
# Task 2
# -----------------------------------------------------------------------------#


def solution_2(Users, Posts):
    users2 = Users[['Id', 'DisplayName']]
    posts2 = Posts[Posts['PostTypeId'] == 1]
    posts2 = posts2.loc[:, ['OwnerUserId', 'ViewCount']]
    posts2 = posts2.groupby("OwnerUserId").sum().reset_index()
    posts2 = posts2.rename({'ViewCount': 'TotalViews', 'OwnerUserId': 'Id'}, axis='columns')
    res = pd.merge(users2, posts2, on='Id')
    res = res.sort_values(by='TotalViews', ascending=False).reset_index(drop=True)
    return res.head(10)



# -----------------------------------------------------------------------------#
# Task 3
# -----------------------------------------------------------------------------#


def solution_3(Badges):
    badgesNames = Badges
    badgesNames['Date'] = pd.to_datetime(badgesNames['Date'])
    badgesNames['Year'] = badgesNames['Date'].dt.year
    badgesNames = badgesNames.groupby(['Year', 'Name']).size().reset_index(name='Count')

    badgesYearly = Badges
    badgesYearly['Date'] = pd.to_datetime(badgesYearly['Date'])
    badgesYearly['Year'] = badgesYearly['Date'].dt.year
    badgesYearly = badgesYearly.groupby('Year').size().reset_index(name='CountTotal')

    mergedBadges = pd.merge(badgesNames, badgesYearly, on='Year')
    mergedBadges = mergedBadges.sort_values(by='Name', ascending=True).reset_index(drop=True)
    mergedBadges['x'] = mergedBadges['Count'] * 1.0 / mergedBadges['CountTotal']

    res = mergedBadges.groupby('Year')['x'].max().reset_index()
    res = pd.merge(res, mergedBadges[['x', 'Name']], on='x')
    res = res.sort_values(by='Year', ascending=True).reset_index(drop=True)
    res = res.rename({'x': 'MaxPercentage'}, axis='columns')
    res = res[['Year', 'Name', 'MaxPercentage']]
    res['Year'] = res['Year'].astype('str')

    return res


# -----------------------------------------------------------------------------#
# Task 4
# -----------------------------------------------------------------------------#


def solution_4(Comments, Posts, Users):
    CmtTotScr = Comments.groupby('PostId')['Score'].sum().reset_index()
    CmtTotScr = CmtTotScr.rename({'PostId': 'Id', 'Score': 'CommentsTotalScore'}, axis='columns')

    posts2 = Posts[Posts['PostTypeId'] == 1]
    postsBestComments = pd.merge(CmtTotScr, posts2, on='Id')
    postsBestComments = postsBestComments.loc[:, ['OwnerUserId', 'Title', 'CommentCount', 'ViewCount',
                                                  'CommentsTotalScore']]

    res = pd.merge(postsBestComments, Users, left_on='OwnerUserId', right_on='Id')
    res = res.sort_values(by='CommentsTotalScore', ascending=False).reset_index(drop=True)
    res = res.loc[:, ['Title', 'CommentCount', 'ViewCount', 'CommentsTotalScore',
                      'DisplayName', 'Reputation', 'Location']]
    return res.head(10)




# -----------------------------------------------------------------------------#
# Task 5
# -----------------------------------------------------------------------------#


def solution_5(Posts, Votes):
    VoteDates = Votes
    VoteDates["year"] = pd.to_datetime(VoteDates["CreationDate"]).dt.year
    VoteDates["VoteDate"] = "before"
    VoteDates.loc[VoteDates["year"] == 2022, "VoteDate"] = "after"
    VoteDates.loc[VoteDates["year"].isin([2019, 2020, 2021]), "VoteDate"] = "during"
    VoteDates = VoteDates[VoteDates["VoteTypeId"].isin([3, 4, 12])][["PostId", "VoteDate"]]
    VoteDates = VoteDates.groupby(["PostId", "VoteDate"]).size().reset_index(name="Total")

    BeforeCovid = VoteDates[VoteDates["VoteDate"] == "before"][["PostId", "Total"]]\
        .rename(columns={"Total": "BeforeCOVIDVotes"})

    DuringCovid = VoteDates[VoteDates["VoteDate"] == "during"][["PostId", "Total"]] \
        .rename(columns={"Total": "DuringCOVIDVotes"})

    AfterCovid = VoteDates[VoteDates["VoteDate"] == "after"][["PostId", "Total"]] \
        .rename(columns={"Total": "AfterCOVIDVotes"})

    VotesByAge = pd.concat([BeforeCovid, DuringCovid, AfterCovid], ignore_index=True).fillna(0)
    VotesByAge["Votes"] = (VotesByAge["BeforeCOVIDVotes"] + \
                          VotesByAge["DuringCOVIDVotes"] + VotesByAge["AfterCOVIDVotes"]).astype(int)

    res = Posts[["Id", "Title", "CreationDate"]].rename(columns={"Id": "PostId", "CreationDate": "Date"})
    res["Date"] = pd.to_datetime(res["Date"]).dt.strftime("%Y-%m-%d")
    res = pd.merge(res, VotesByAge, on='PostId')
    res['BeforeCOVIDVotes'] = res['BeforeCOVIDVotes'].astype(int)
    res['DuringCOVIDVotes'] = res['DuringCOVIDVotes'].astype(int)

    res['AfterCOVIDVotes'] = res['AfterCOVIDVotes'].astype(int)

    res = res[res['Title'].notnull()]
    res = res[res["DuringCOVIDVotes"] > 0]
    res = res.sort_values(by=["DuringCOVIDVotes", "Votes"], ascending=[False, False]).reset_index(drop=True)
    column = res.pop("PostId")
    res.insert(2, "PostId", column)
    return res.head(20)





