import { AuthProvider } from "@/core/auth/AuthProvider";
import { WorkspaceContent } from "./workspace-content";

export const dynamic = "force-dynamic";

// Mock user — bypasses login/setup. The backend stubs return the same user.
const mockUser = {
  id: "demo-user",
  email: "demo@deer-flow.local",
  display_name: "Demo User",
  is_admin: true,
  needs_setup: false,
  system_role: "admin" as const,
  profile_image_url: "",
};

export default async function WorkspaceLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  // Auth bypassed — always render with a fixed user.
  // Real auth is available via the backend at /api/v1/auth/* if needed.
  return (
    <AuthProvider initialUser={mockUser}>
      <WorkspaceContent>{children}</WorkspaceContent>
    </AuthProvider>
  );
}
