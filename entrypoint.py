import os
import asyncio
import tempfile
from git import Repo, Actor
from core.components.plugins.logic.models import ModuleManifest

manifest = ModuleManifest(
    id="lyndrix.service.git",
    name="Git Service",
    version="0.1.2",
    description="Headless Service zur Verwaltung von Git-Repositories (Remote & Local).",
    type="PLUGIN",
    permissions={"subscribe": ["git:sync", "git:commit_push"], "emit": ["git:status_update"]}
)

class GitManager:
    def __init__(self, ctx):
        self.ctx = ctx
        self.base_dir = "/data/storage/git_repos"
        os.makedirs(self.base_dir, exist_ok=True)
        self.ctx.log.debug(f"[GIT:INIT] GitManager initialized. Base storage dir: {self.base_dir}")

    def get_repo_path(self, repo_id):
        return os.path.join(self.base_dir, repo_id)

    # --- BLOCKING HELPER METHODS (Run in Executor) ---
    def _init_local_blocking(self, path, repo_id):
        self.ctx.log.debug(f"[GIT:{repo_id}] Checking for existing .git directory at {path}")
        if not os.path.exists(os.path.join(path, ".git")):
            self.ctx.log.info(f"[GIT:{repo_id}] Initializing new local repository at {path}")
            Repo.init(path)
            self.ctx.log.info(f"[GIT:{repo_id}] Local repository initialized successfully.")
        else:
            self.ctx.log.debug(f"[GIT:{repo_id}] Local repository already exists at {path}. Ready for operations.")

    def _sync_https_blocking(self, url, token, path, repo_id):
        self.ctx.log.debug(f"[GIT:{repo_id}] Formatting authentication URL for HTTPS sync...")
        auth_url = url.replace("https://", f"https://oauth2:{token}@") if token else url
        
        self.ctx.log.debug(f"[GIT:{repo_id}] Checking if repository exists at {path}")
        if not os.path.exists(os.path.join(path, ".git")):
            self.ctx.log.info(f"[GIT:{repo_id}] Cloning HTTPS repository from {url}...")
            Repo.clone_from(auth_url, path)
            self.ctx.log.info(f"[GIT:{repo_id}] HTTPS clone completed successfully.")
        else:
            self.ctx.log.info(f"[GIT:{repo_id}] Existing repository found. Pulling latest HTTPS changes...")
            Repo(path).remotes.origin.pull()
            self.ctx.log.info(f"[GIT:{repo_id}] HTTPS pull completed successfully.")

    # --- ASYNC HANDLERS ---
    async def handle_sync(self, payload):
        repo_id = payload.get("repo_id")
        url = payload.get("url")
        auth_type = payload.get("auth_type", "none")
        secret_value = payload.get("secret_value")
        path = self.get_repo_path(repo_id)

        self.ctx.log.info(f"[GIT:SYNC] >> Starting sync pipeline for repository '{repo_id}'")
        self.ctx.log.debug(f"[GIT:SYNC] Payload specifics: url='{url}', auth_type='{auth_type}', target_path='{path}'")

        try:
            loop = asyncio.get_event_loop()
            
            if not url:
                # LOCAL MODE
                self.ctx.log.info(f"[GIT:{repo_id}] Mode selected: LOCAL (No remote URL provided)")
                await loop.run_in_executor(None, self._init_local_blocking, path, repo_id)
            elif auth_type == "ssh":
                # SSH MODE (Secure)
                self.ctx.log.info(f"[GIT:{repo_id}] Mode selected: SSH (Secure Key Authentication)")
                await self._sync_ssh(url, secret_value, path, repo_id)
            else:
                # TOKEN MODE (Standard HTTPS)
                self.ctx.log.info(f"[GIT:{repo_id}] Mode selected: HTTPS (Token Authentication)")
                await loop.run_in_executor(None, self._sync_https_blocking, url, secret_value, path, repo_id)
            
            self.ctx.log.info(f"[GIT:SYNC] << Sync pipeline for '{repo_id}' completed successfully.")
            # Use ctx.emit to respect security permissions
            self.ctx.log.debug(f"[GIT:SYNC] Emitting 'git:status_update' for '{repo_id}' with status: synced")
            self.ctx.emit("git:status_update", {"repo_id": repo_id, "status": "synced"})
        except Exception as e:
            self.ctx.log.error(f"[GIT:SYNC] !! FATAL ERROR during sync for '{repo_id}': {str(e)}")
            self.ctx.emit("git:status_update", {"repo_id": repo_id, "status": "error", "error": str(e)})

    async def _sync_ssh(self, url, private_key, path, repo_id):
        # We must offload the SSH clone to a thread as well, otherwise it blocks the loop
        def _ssh_clone_blocking():
            self.ctx.log.debug(f"[GIT:{repo_id}] Creating secure temporary file for SSH key...")
            with tempfile.NamedTemporaryFile(mode='w', delete=True) as f:
                f.write(private_key)
                f.flush()
                os.chmod(f.name, 0o600)
                self.ctx.log.debug(f"[GIT:{repo_id}] Temporary SSH key saved and secured with 0600 permissions.")
                
                env = os.environ.copy()
                env["GIT_SSH_COMMAND"] = f"ssh -i {f.name} -o StrictHostKeyChecking=no"
                self.ctx.log.debug(f"[GIT:{repo_id}] GIT_SSH_COMMAND environment variable configured.")
                
                if not os.path.exists(os.path.join(path, ".git")):
                    self.ctx.log.info(f"[GIT:{repo_id}] Cloning SSH repository from {url}...")
                    Repo.clone_from(url, path, env=env)
                    self.ctx.log.info(f"[GIT:{repo_id}] SSH clone completed successfully.")
                else:
                    self.ctx.log.info(f"[GIT:{repo_id}] Existing repository found. Pulling latest SSH changes...")
                    Repo(path).remotes.origin.pull(env=env)
                    self.ctx.log.info(f"[GIT:{repo_id}] SSH pull completed successfully.")
                    
            self.ctx.log.debug(f"[GIT:{repo_id}] Temporary SSH key file securely deleted.")

        loop = asyncio.get_event_loop()
        self.ctx.log.debug(f"[GIT:{repo_id}] Dispatching SSH operation to thread pool executor...")
        await loop.run_in_executor(None, _ssh_clone_blocking)

    def _commit_blocking(self, repo_id, message, path, is_local):
        self.ctx.log.debug(f"[GIT:{repo_id}] Initializing Git object for path: {path}")
        repo = Repo(path)
        
        self.ctx.log.debug(f"[GIT:{repo_id}] Adding all files to staging area (git add A=True)...")
        repo.git.add(A=True)
        
        self.ctx.log.debug(f"[GIT:{repo_id}] Evaluating repository for uncommitted changes...")
        if repo.is_dirty(untracked_files=True):
            self.ctx.log.info(f"[GIT:{repo_id}] Changes detected in working tree. Preparing commit.")
            author = Actor("Lyndrix IaC", "iac@lyndrix.local")
            
            self.ctx.log.debug(f"[GIT:{repo_id}] Committing changes with message: '{message}'")
            repo.index.commit(message, author=author, committer=author)
            self.ctx.log.info(f"[GIT:{repo_id}] Successfully created local commit.")
            
            if not is_local and repo.remotes:
                self.ctx.log.info(f"[GIT:{repo_id}] Remote is configured (is_local=False). Pushing changes to origin...")
                repo.remotes.origin.push()
                self.ctx.log.info(f"[GIT:{repo_id}] Successfully pushed changes to remote.")
                return "pushed"
                
            self.ctx.log.info(f"[GIT:{repo_id}] Keeping changes local (is_local=True or no remotes configured).")
            return "committed_locally"
            
        self.ctx.log.info(f"[GIT:{repo_id}] Working tree is perfectly clean. No commit necessary.")
        return "no_changes"

    async def handle_commit_push(self, payload):
        repo_id = payload.get("repo_id")
        message = payload.get("message", "Update via Lyndrix IaC GUI")
        is_local = payload.get("is_local", False)
        path = self.get_repo_path(repo_id)

        self.ctx.log.info(f"[GIT:COMMIT] >> Starting commit/push pipeline for repository '{repo_id}'")
        self.ctx.log.debug(f"[GIT:COMMIT] Payload specifics: is_local={is_local}, message='{message}'")

        try:
            loop = asyncio.get_event_loop()
            self.ctx.log.debug(f"[GIT:COMMIT] Dispatching commit operation to thread pool executor...")
            result = await loop.run_in_executor(None, self._commit_blocking, repo_id, message, path, is_local)
            
            self.ctx.log.info(f"[GIT:COMMIT] << Pipeline finished for '{repo_id}' with result: {result}")
            # Use ctx.emit, NEVER use raw bus.emit in a plugin
            self.ctx.log.debug(f"[GIT:COMMIT] Emitting 'git:status_update' for '{repo_id}' with status: {result}")
            self.ctx.emit("git:status_update", {"repo_id": repo_id, "status": result})
        except Exception as e:
            self.ctx.log.error(f"[GIT:COMMIT] !! FATAL ERROR during commit/push for '{repo_id}': {str(e)}")
            self.ctx.emit("git:status_update", {"repo_id": repo_id, "status": "error", "error": str(e)})

def setup(ctx):
    ctx.log.info("[GIT:SETUP] Starting Git Service initialization sequence...")
    manager = GitManager(ctx)
    
    ctx.log.debug("[GIT:SETUP] Subscribing to event 'git:sync'...")
    ctx.subscribe("git:sync")(manager.handle_sync)
    
    ctx.log.debug("[GIT:SETUP] Subscribing to event 'git:commit_push'...")
    ctx.subscribe("git:commit_push")(manager.handle_commit_push)
    
    ctx.log.info("[GIT:SETUP] Git Service is fully initialized and actively listening for events.")