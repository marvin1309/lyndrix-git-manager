import os
import asyncio
import tempfile
from git import Repo, Actor
from core.components.plugins.logic.models import ModuleManifest

manifest = ModuleManifest(
    id="lyndrix.service.git",
    name="Git Service",
    version="1.1.0",
    description="Headless Service zur Verwaltung von Git-Repositories (Remote & Local).",
    type="PLUGIN",
    permissions={"subscribe": ["git:sync", "git:commit_push"], "emit": ["git:status_update"]}
)

class GitManager:
    def __init__(self, ctx):
        self.ctx = ctx
        self.base_dir = "/data/storage/git_repos"
        os.makedirs(self.base_dir, exist_ok=True)

    def get_repo_path(self, repo_id):
        return os.path.join(self.base_dir, repo_id)

    # --- BLOCKING HELPER METHODS (Run in Executor) ---
    def _init_local_blocking(self, path, repo_id):
        if not os.path.exists(os.path.join(path, ".git")):
            self.ctx.log.info(f"GIT: Initializing local repository at {path}")
            Repo.init(path)
        else:
            self.ctx.log.info(f"GIT: Local repository {repo_id} ready.")

    def _sync_https_blocking(self, url, token, path, repo_id):
        auth_url = url.replace("https://", f"https://oauth2:{token}@") if token else url
        if not os.path.exists(os.path.join(path, ".git")):
            self.ctx.log.info(f"GIT: Cloning HTTPS repository {repo_id}...")
            Repo.clone_from(auth_url, path)
        else:
            self.ctx.log.info(f"GIT: Pulling latest HTTPS changes for {repo_id}...")
            Repo(path).remotes.origin.pull()

    # --- ASYNC HANDLERS ---
    async def handle_sync(self, payload):
        repo_id = payload.get("repo_id")
        url = payload.get("url")
        auth_type = payload.get("auth_type", "none")
        secret_value = payload.get("secret_value")
        path = self.get_repo_path(repo_id)

        try:
            loop = asyncio.get_event_loop()
            
            if not url:
                # LOCAL MODE
                await loop.run_in_executor(None, self._init_local_blocking, path, repo_id)
            elif auth_type == "ssh":
                # SSH MODE (Secure)
                await self._sync_ssh(url, secret_value, path)
            else:
                # TOKEN MODE (Standard HTTPS)
                await loop.run_in_executor(None, self._sync_https_blocking, url, secret_value, path, repo_id)
            
            # Use ctx.emit to respect security permissions
            self.ctx.emit("git:status_update", {"repo_id": repo_id, "status": "synced"})
        except Exception as e:
            self.ctx.log.error(f"GIT_ERROR: {e}")
            self.ctx.emit("git:status_update", {"repo_id": repo_id, "status": "error", "error": str(e)})

    async def _sync_ssh(self, url, private_key, path):
        # We must offload the SSH clone to a thread as well, otherwise it blocks the loop
        def _ssh_clone_blocking():
            with tempfile.NamedTemporaryFile(mode='w', delete=True) as f:
                f.write(private_key)
                f.flush()
                os.chmod(f.name, 0o600)
                
                env = os.environ.copy()
                env["GIT_SSH_COMMAND"] = f"ssh -i {f.name} -o StrictHostKeyChecking=no"
                
                if not os.path.exists(os.path.join(path, ".git")):
                    self.ctx.log.info(f"GIT: Cloning SSH repository...")
                    Repo.clone_from(url, path, env=env)
                else:
                    self.ctx.log.info(f"GIT: Pulling latest SSH changes...")
                    Repo(path).remotes.origin.pull(env=env)

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _ssh_clone_blocking)

    def _commit_blocking(self, repo_id, message, path, is_local):
        repo = Repo(path)
        repo.git.add(A=True)
        
        if repo.is_dirty(untracked_files=True):
            author = Actor("Lyndrix IaC", "iac@lyndrix.local")
            repo.index.commit(message, author=author, committer=author)
            
            if not is_local and repo.remotes:
                repo.remotes.origin.push()
                return "pushed"
            return "committed_locally"
            
        return "no_changes"

    async def handle_commit_push(self, payload):
        repo_id = payload.get("repo_id")
        message = payload.get("message", "Update via Lyndrix IaC GUI")
        is_local = payload.get("is_local", False)
        path = self.get_repo_path(repo_id)

        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self._commit_blocking, repo_id, message, path, is_local)
            
            self.ctx.log.info(f"GIT: Operation result for {repo_id}: {result}")
            # Use ctx.emit, NEVER use raw bus.emit in a plugin
            self.ctx.emit("git:status_update", {"repo_id": repo_id, "status": result})
        except Exception as e:
            self.ctx.log.error(f"GIT_ERROR: Commit/Push failed for {repo_id}: {e}")
            self.ctx.emit("git:status_update", {"repo_id": repo_id, "status": "error", "error": str(e)})

def setup(ctx):
    manager = GitManager(ctx)
    ctx.subscribe("git:sync")(manager.handle_sync)
    ctx.subscribe("git:commit_push")(manager.handle_commit_push)