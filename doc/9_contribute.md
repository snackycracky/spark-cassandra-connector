# Documentation

## Contributing
Before starting to work on a feature or a fix, make sure that:
- There is a ticket for your work in the project's issue tracker. If not, simply create it first.
- Use the ticket number in your branch name.
- For features (vs tasks, bugs, minor changes) the ticket has been scheduled for a milestone (ask the team if you are creating it). 

### Getting Started
 - You should always perform your work in a Git feature branch. 
 - The branch should be given a descriptive name that includes the ticket number and explains its intent (see more below).
 - When the feature or fix is completed you should open a Pull Request on GitHub.
 - The Pull Request should be reviewed by other maintainers. Outside contributors are encouraged to participate in the review process, it is not a closed process.
 - After the review you should fix the issues as needed (pushing a new commit for new review etc.), iterating until the reviewers give their thumbs up. When the branch conflicts with its merge target (either by way of git merge conflict or failing CI tests), do not merge the target branch into your feature branch. Instead rebase your branch onto the target branch. Merges complicate the git history, especially for the squashing which is necessary later (see below).
 - Once the code has passed review the Pull Request can be merged into the master branch. For this purpose the commits which were added on the feature branch should be squashed into a single commit. This can be done using the command git rebase -i master (or the appropriate target branch), picking the first commit and squashing all following ones. 
 - Also make sure that the commit message conforms to the syntax specified below.
 - If the code change needs to be applied to other branches as well, create pull requests against those branches which contain the change after rebasing it onto the respective branch and await successful verification by the continuous integration infrastructure; then merge those pull requests.
 - Please mark these pull requests (needs review) in the title to make the purpose clear in the pull request list.
 - Once everything is said and done, associate the ticket with the “earliest” release milestone (i.e. if back-ported so that it will be in release x.y.z, find the relevant milestone for that release) and close it.

### Work In Progress
It is ok to work on a public feature branch in the GitHub repository. Something that can sometimes be useful for early feedback etc. If so then it is preferable to name the branch accordingly. 

This can be done by either:
 - Prefix the name with wip- as in ‘Work In Progress’
 - Use hierarchical names like wip-.., feature-.. or task-... 

Either way is fine as long as it is clear that it is work in progress and not ready for merge. 

To facilitate both well-formed commits and working together, the wip and feature/topic identifiers also have special meaning. Any branch labelled with wip is considered “git-unstable” and may be rebased and have its history rewritten. Any branch with feature/topic in the name is considered “stable” enough for others to depend on when a group is working on a feature.

### Commits
If your work spans multiple local commits (for example; if you do safe point commits while working in a feature branch or work in a branch for long time doing merges/rebases etc.) then please do not commit it all but rewrite the history by squashing the commits into a single big commit which you write a good commit message for (like discussed in the following sections). 
For more info read this article: [Git Workflow](http://sandofsky.com/blog/git-workflow.html). Every commit should be able to be used in isolation, cherry picked etc.
First line should be a descriptive sentence what the commit is doing. It should be possible to fully understand what the commit does—but not necessarily how it does it—by just reading this single line. We follow the “imperative present tense” style for commit messages ([more info here](http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html)).

### External Dependencies
All the external runtime dependencies for the project, including transitive dependencies, must have an open source license that is equal to, or compatible with, [Apache 2](http://www.apache.org/licenses/LICENSE-2.0).

### Pull Request Requirements
For a Pull Request to be considered at all it has to meet these requirements:
 - Live up to or better the current code standard:
 - Not violate [DRY](http://programmer.97things.oreilly.com/wiki/index.php/Don%27t_Repeat_Yourself).
 - [Boy Scout Rule](http://programmer.97things.oreilly.com/wiki/index.php/The_Boy_Scout_Rule) needs to have been applied.
 - Regardless if the code introduces new features or fixes bugs or regressions, it must have comprehensive tests.
 - The code must be well documented.
 - The commit messages must properly describe the changes. 

If these requirements are not met then the code should not be merged into master. 

And finally, thanks and have fun!