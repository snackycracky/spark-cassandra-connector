# Documentation

## Contributing
- Pick a ticket in the project's issue tracker, assign yourself and put `In Progress` label on it - see [Picking a ticket](#picking-a-ticket)
- Create a branch and commit your changes - see [Branch and Commits](#branch-and-commits)
- Create a pull request and put `Needs review` label once it is ready to be reviewed - see [Pull Request](#pull-request)
- Wait for the team to review your changes - see [Review and Merge](#review-and-merge)

### <a name="picking-a-ticket"/>Picking a Ticket
The ticket you want to work on must be scheduled for a milestone. It means that the team accepted the ticket and wants it to be a part of the release.
If there is no ticket for the feature you want to work on, create one and put one of the following labels on it:
- `Bug` - if it is a bug
- `Feature` - if it is a completely new feature
- `Enhancement` - if it is an improvement of the current functionality (e.g. usability or performance improvement)
- `Task` - if it is a refactoring or other code change related task
- `Documentation` - if it is a documentation related task 

You should also put `Needs review` label on it so that the team will review it and:
- schedule it for a milestone - good for you, you can start to work on the ticket
- put one of the following labels: `Won't Fix`, `Invalid`, `Duplicate` - working on the ticket is a waste of time
- put `future` label - you may start to work on the ticket, however we cannot guarantee when your contribution will be merged (if at all)
     
### <a name="branch-and-commits"/>Branch and Commits

#### Branches
It is ok to work on a public feature branch in the GitHub repository. Something that can sometimes be useful for early feedback etc. If so then it is preferable to name the branch accordingly. 

This can be done by either:
 - prefix the name with wip-xxx- as in ‘Work In Progress’ (xxx for the ticket number)
 - use hierarchical names like: type/xxx-name, where type is one of the following: bugfix, feature, enhancement, task, doc - depending on the label assigned to the ticket 

To facilitate both well-formed commits and working together, the wip and type/topic identifiers also have special meaning. Any branch labelled with wip is considered “git-unstable” and may be rebased and have its history rewritten. Any branch with type/topic in the name is considered “stable” enough for others to depend on when a group is working on a feature.

#### Commits
If your work spans multiple local commits (for example; if you do safe point commits while working in a feature branch or work in a branch for long time doing merges/rebases etc.) then please do not commit it all but rewrite the history by squashing the commits into a single big commit which you write a good commit message for (like discussed in the following sections). 
For more info read this article: [Git Workflow](http://sandofsky.com/blog/git-workflow.html). Every commit should be able to be used in isolation, cherry picked etc.
First line should be a descriptive sentence what the commit is doing. It should be possible to fully understand what the commit does—but not necessarily how it does it—by just reading this single line. We follow the “imperative present tense” style for commit messages ([more info here](http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html)).

### <a name="pull-request"/>Pull Request
For a Pull Request to be considered at all it has to meet these requirements:
 - Live up to or better the current code standard:
 - Not violate [DRY](http://programmer.97things.oreilly.com/wiki/index.php/Don%27t_Repeat_Yourself).
 - [Boy Scout Rule](http://programmer.97things.oreilly.com/wiki/index.php/The_Boy_Scout_Rule) needs to have been applied.
 - Regardless if the code introduces new features or fixes bugs or regressions, it must have comprehensive tests.
 - The code must be well documented.
 - The commit messages must properly describe the changes. 

If these requirements are not met then the code should not be merged.

You should also add the introduced changes on top of the `CHANGES.txt` file.

### <a name="review-and-merge"/>Review and Merge
When the feature or fix is completed you should open a Pull Request on GitHub. The Pull Request must be reviewed by other maintainers (you should put `Needs Review` label on it once it is ready). Outside contributors are encouraged to participate in the review process, it is not a closed process.
Once the review is complete, the reviewer adds the appropriate label:
- `Needs Changes` - it means that your implementation needs some changes; once you do them, remove `Needs Changes` label and add `Needs Review` again
- `Needs Squashing` - it means that your implementation has been accepted; however, you need to make your PR mergeable and/or squash the commits 
- `Needs Testing` - it means that your implementation has been accepted and it waits for someone to test it
- `Ready To Merge` - it means that your implementation has been accepted and doesn't require any additional testing

When the branch conflicts with its merge target (either by way of git merge conflict or failing CI tests), *do not merge the target branch into your feature branch*. Instead rebase your branch onto the target branch. Merges complicate the git history, especially for the squashing which is necessary later.
Commits which were added on the feature branch should be squashed into a single commit. This can be done using the command `git rebase -i master` (or the appropriate target branch), picking the first commit and squashing all following ones. 
If the code change needs to be applied to other branches as well, create pull requests against those branches which contain the change after rebasing it onto the respective branch and await successful verification by the continuous integration infrastructure. Those pull requests must also be reviewed by maintainers before they are merged, therefore you should add `Needs Review` label to them.

### External Dependencies
All the external runtime dependencies for the project, including transitive dependencies, must have an open source license that is equal to, or compatible with, [Apache 2](http://www.apache.org/licenses/LICENSE-2.0).

And finally, thanks and have fun!
