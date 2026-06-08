import { redirect } from "next/navigation";
import { type ReactNode } from "react";

export const dynamic = "force-dynamic";

export default async function AuthLayout({
  children,
}: {
  children: ReactNode;
}) {
  // Auth bypassed — redirect all auth pages to workspace.
  redirect("/workspace");
}
