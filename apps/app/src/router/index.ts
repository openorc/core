import {
  createRouter,
  createWebHistory,
  type Router,
  type RouterHistory,
  type RouteRecordRaw,
} from 'vue-router'
import HomeView from '../views/HomeView.vue'

// The root route proves the shell builds and runs. Product routes (dashboard,
// Task wizards, Owner Requests, telemetry) arrive in later milestones.
export const routes: RouteRecordRaw[] = [
  {
    path: '/',
    name: 'home',
    component: HomeView,
  },
]

export function createAppRouter(
  history: RouterHistory = createWebHistory(import.meta.env.BASE_URL),
): Router {
  return createRouter({
    history,
    routes,
  })
}

export const router: Router = createAppRouter()